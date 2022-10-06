use std::net::UdpSocket;

fn main() -> std::io::Result<()> {
    let socket = UdpSocket::bind("0.0.0.0:8000")?;
    let mut buf = [0; 1000];

    let mut db = std::collections::HashMap::<String, String>::new();

    while let Ok((amt, src)) = socket.recv_from(&mut buf) {
        let buf = &mut buf[..amt];

        let text = String::from_utf8_lossy(&buf);
        let equals_position = text.chars().position(|x| x == '=');

        println!("{} sent {} ", src, text);

        if let Some(equals_position) = equals_position {
            let key = &text[..equals_position];
            if key == "version" {
                continue;
            }
            let value = &text[equals_position+1..];
            println!("setting key:'{}' value:'{}'", key, value);
            _ = db.insert(key.to_owned(), value.to_owned());
        } else {
            if text == "version" {
                socket.send_to("version=unusual-db 0.0.1".as_bytes(), &src)?;    
                continue;
            }
            let empty = "".to_string();
            let value = db.get(&text.to_string()).unwrap_or(&empty);
            println!("getting key:'{}' value:'{}'", text, value);
            socket.send_to(format!("{}={}", text, value).as_bytes(), &src)?;
        }
    }

    Ok(())
}
