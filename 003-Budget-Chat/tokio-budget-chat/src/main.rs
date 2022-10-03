pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub type Result<T> = std::result::Result<T, Error>;

use bytes::{Buf, BytesMut};
use itertools::Itertools;
use std::future::Future;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::signal;
use tokio::sync::{broadcast, mpsc, Semaphore};
use tokio::time::{self, Duration};

#[derive(Debug)]
struct DbDropGuard {
    db: Db,
}

impl DbDropGuard {
    pub fn new() -> Self {
        Self { db: Db::new() }
    }

    pub fn db(&self) -> Db {
        self.db.clone()
    }
}

#[derive(Debug, Clone)]
struct Db {
    shared: Arc<Shared>,
}

impl Db {
    pub fn new() -> Self {
        let shared = Arc::new(Shared {
            state: Mutex::new(State {
                users: HashMap::new(),
            }),
        });

        Db { shared }
    }

    pub fn with_users<T>(&mut self, f: impl FnOnce(&mut HashMap<SocketAddr, String>) -> T) -> T {
        let mut state = self.shared.state.lock().unwrap();
        f(&mut state.users)
    }

    pub fn print(&mut self) {
        self.with_users(|users| {
            println!("{:?}", users);
        });
    }
}

#[derive(Debug)]
struct Shared {
    state: Mutex<State>,
}

use std::collections::HashMap;

#[derive(Debug)]
struct State {
    users: HashMap<SocketAddr, String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let port = 8000;
    let listener = TcpListener::bind(&format!("0.0.0.0:{}", port)).await?;
    run(listener, signal::ctrl_c()).await;
    Ok(())
}

const MAX_CONNECTIONS: usize = 10;

pub async fn run(listener: TcpListener, shutdown: impl Future) {
    let (notify_shutdown, _) = broadcast::channel(1);
    let (shutdown_complete_tx, shutdown_complete_rx) = mpsc::channel(1);
    let (chat_tx, _chat_rx) = broadcast::channel(1);

    let mut listener = Listener {
        listener,
        db_holder: DbDropGuard::new(),
        limit_connections: Arc::new(Semaphore::new(MAX_CONNECTIONS)),
        notify_shutdown,
        shutdown_complete_tx,
        shutdown_complete_rx,
        chat_tx,
    };

    tokio::select! {
        res = listener.run() => {
            if let Err(err) = res {
                println!("failed to accept {}", err);
            }
        }
        _ = shutdown => {
            println!("shutting down");
        }
    }

    let Listener {
        mut shutdown_complete_rx,
        shutdown_complete_tx,
        notify_shutdown,
        ..
    } = listener;

    drop(notify_shutdown);
    drop(shutdown_complete_tx);
    let _ = shutdown_complete_rx.recv().await;
}

#[derive(Debug)]
struct Listener {
    db_holder: DbDropGuard,
    listener: TcpListener,
    limit_connections: Arc<Semaphore>,
    notify_shutdown: broadcast::Sender<()>,
    shutdown_complete_tx: mpsc::Sender<()>,
    shutdown_complete_rx: mpsc::Receiver<()>,
    chat_tx: broadcast::Sender<BroadcastMessage>,
}

impl Listener {
    async fn run(&mut self) -> crate::Result<()> {
        println!("accepting inbound connections");

        loop {
            let permit = self
                .limit_connections
                .clone()
                .acquire_owned()
                .await
                .unwrap();

            let socket = self.accept().await?;

            let mut handler = Handler {
                nickname: None,
                db: self.db_holder.db(),
                connection: Connection::new(socket),
                shutdown: Shutdown::new(self.notify_shutdown.subscribe()),
                _shutdown_complete: self.shutdown_complete_tx.clone(),
                chat_tx: self.chat_tx.clone(),
                chat_rx: self.chat_tx.subscribe(),
            };

            println!("{} connected", handler.connection.peer_addr());
            handler.db.print();

            tokio::spawn(async move {
                let peer_addr = handler.connection.peer_addr();
                if let Err(err) = handler.run().await {
                    println!("{} connection error {}", peer_addr, err);
                    handler.db.print();
                    drop(permit);
                }
                println!("{} disconnected", handler.connection.peer_addr());
                if let Some(nickname) = handler.nickname.take() {
                    _ = handler.chat_tx.send(BroadcastMessage {
                        bytes: format!("* {} has left the room", nickname),
                        sender: peer_addr,
                    });

                    handler.db.with_users(|users| {
                        users.remove(&peer_addr);
                    });
                }
            });
        }
    }

    async fn accept(&mut self) -> crate::Result<TcpStream> {
        let mut backoff = 1;

        loop {
            match self.listener.accept().await {
                Ok((socket, _)) => return Ok(socket),
                Err(err) => {
                    if backoff > 64 {
                        return Err(err.into());
                    }
                }
            }

            time::sleep(Duration::from_secs(backoff)).await;

            backoff *= 2;
        }
    }
}

#[derive(Debug)]
struct Handler {
    pub nickname: Option<String>,
    pub db: Db,
    pub connection: Connection,
    shutdown: Shutdown,
    _shutdown_complete: mpsc::Sender<()>,
    chat_tx: broadcast::Sender<BroadcastMessage>,
    chat_rx: broadcast::Receiver<BroadcastMessage>,
}

#[derive(Debug, Clone)]
struct BroadcastMessage {
    bytes: String,
    sender: SocketAddr,
}

impl Handler {
    async fn run(&mut self) -> crate::Result<()> {
        self.connection
            .write("Welcome to budgetchat! What shall I call you?")
            .await?;
        while !self.shutdown.is_shutdown() {
            tokio::select! {
                bcast = self.chat_rx.recv() => {
                    let bcast = bcast?;
                    if self.nickname.is_some() && bcast.sender != self.connection.peer_addr() {
                        self.connection.write(&bcast.bytes).await?;
                    }
                }
                res = self.connection.read_frame() => {
                    let frame = match res? {
                        Some(frame) => frame,
                        None => return Ok(()),
                    };

                    println!("{} sent {}", self.connection.peer_addr(), frame);
                    self.db.print();

                    if self.nickname.is_none() {
                        if is_valid_nickname(&frame) {
                            self.nickname = Some(frame.clone());
                            let users = self.db.with_users(|users| {
                                let current_users = users
                                    .values()
                                    .map(|x| x.as_str());
                                let current_users = Itertools::intersperse(current_users, ", ");
                                let current_users: String = current_users.collect();
                                _ = users.insert(self.connection.peer_addr(), frame.clone());
                                current_users
                            });
                            self.chat_tx.send(BroadcastMessage {
                                bytes: format!("* {} has entered the room.", &frame),
                                sender: self.connection.peer_addr()
                            })?;
                            self.connection.write(&format!("* The room contains: {}", users)).await?;
                            continue;
                        } else {
                            return Ok(());
                        }
                    }

                    self.chat_tx.send(BroadcastMessage {
                        bytes: format!("[{}] {}", self.nickname.as_ref().unwrap(), frame),
                        sender: self.connection.peer_addr()
                    })?;
                }
                _ = self.shutdown.recv() => {
                    return Ok(());
                }
            };
        }

        Ok(())
    }
}

fn is_valid_nickname(nickname: &str) -> bool {
    nickname.len() > 0 && nickname.len() <= 16 && nickname.is_ascii()
}

#[derive(Debug)]
struct Connection {
    peer_addr: SocketAddr,
    stream: BufWriter<TcpStream>,
    buffer: BytesMut,
}

impl Connection {
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            peer_addr: socket.peer_addr().unwrap(),
            stream: BufWriter::new(socket),
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    pub fn peer_addr(&self) -> SocketAddr {
        self.peer_addr
    }

    pub async fn write(&mut self, what: &str) -> std::io::Result<()> {
        self.stream.write(what.as_bytes()).await?;
        self.stream.write(&[b'\n']).await?;
        self.stream.flush().await
    }

    pub async fn read_frame(&mut self) -> crate::Result<Option<String>> {
        loop {
            if let Some(frame) = self.parse_frame() {
                return Ok(Some(frame));
            }

            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    fn parse_frame(&mut self) -> Option<String> {
        let mut buf = Cursor::new(&self.buffer[..]);

        match get_line(&mut buf) {
            Some(line) => {
                let len = buf.position() as usize;
                buf.set_position(0);
                let line = String::from_utf8_lossy(&line).to_string();
                self.buffer.advance(len);
                Some(line)
            }
            None => None,
        }
    }
}

fn get_line<'a>(src: &mut Cursor<&'a [u8]>) -> Option<&'a [u8]> {
    let start = src.position() as usize;
    let end = src.get_ref().len();

    for i in start..end {
        if src.get_ref()[i] == b'\n' {
            src.set_position(i as u64 + 1);

            return Some(&src.get_ref()[start..i]);
        }
    }

    None
}

#[derive(Debug)]
struct Shutdown {
    shutdown: bool,
    notify: broadcast::Receiver<()>,
}

impl Shutdown {
    pub fn new(notify: broadcast::Receiver<()>) -> Self {
        Self {
            shutdown: false,
            notify,
        }
    }

    pub fn is_shutdown(&self) -> bool {
        self.shutdown
    }

    pub async fn recv(&mut self) {
        if self.shutdown {
            return;
        }

        let _ = self.notify.recv().await;

        self.shutdown = true;
    }
}
