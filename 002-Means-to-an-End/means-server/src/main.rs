use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use rusqlite::Connection;
use std::io::Read;
use std::{net::TcpListener, thread};

fn main() {
    let port = 8000;

    let listener = TcpListener::bind(&format!("0.0.0.0:{}", port)).unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        thread::spawn(|| {
            let peer = stream.peer_addr().unwrap();
            println!("Connected {}:{}", peer.ip(), peer.port());
            handle_connection(stream);
            println!("Disconnected {}:{}", peer.ip(), peer.port());
        });
    }
}

fn handle_connection(mut stream: std::net::TcpStream) {
    let db = Connection::open_in_memory().unwrap();
    db.execute_batch("CREATE TABLE means (timestamp INTEGER, price INTEGER);")
        .unwrap();

    let mut read_buf = [0; 9];

    loop {
        match stream.read_exact(&mut read_buf) {
            Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return,
            Err(ref e) => panic!("{}", e),
            _ => (),
        };

        let mut reader = std::io::Cursor::new(read_buf);

        let msg_type = reader.read_u8().unwrap();
        let first_num = reader.read_i32::<BigEndian>().unwrap();
        let second_num = reader.read_i32::<BigEndian>().unwrap();

        let peer = stream.peer_addr().unwrap();
        println!("Got {} {} {} from {}:{}", msg_type as char, first_num, second_num, peer.ip(), peer.port());

        match msg_type as char {
            'I' => {
                let timestamp = first_num;
                let price = second_num;
                db.execute("INSERT INTO means VALUES (?, ?)", [timestamp, price])
                    .unwrap();
            }
            'Q' => {
                let min_time = first_num;
                let max_time = second_num;
                let avg: i32 = db
                    .query_row(
                        "SELECT avg(price) FROM means WHERE timestamp BETWEEN ? AND ?",
                        [min_time, max_time],
                        |row| row.get(0),
                    )
                    .unwrap();
                stream.write_i32::<BigEndian>(avg).unwrap();
            }
            c => panic!("Unknown type {}", c),
        }
    }
}
