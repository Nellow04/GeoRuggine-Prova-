use chrono::Utc;
use rand::Rng;
use shared::{Coordinates, Message};
use std::env;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let username = if args.len() > 1 {
        args[1].clone()
    } else {
        format!("Veicolo_{}", rand::thread_rng().gen_range(1000..9999))
    };

    println!("Avvio client come {}", username);
    
    let mut stream = TcpStream::connect("127.0.0.1:8080").await?;
    println!("Connesso al server.");

    // 1. Auth Phase
    let auth_msg = Message::AuthRequest { username: username.clone() };
    let json_auth = serde_json::to_string(&auth_msg)? + "\n";
    stream.write_all(json_auth.as_bytes()).await?;

    let (read_half, mut write_half) = stream.into_split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    let bytes_read = reader.read_line(&mut line).await?;
    if bytes_read == 0 {
        eprintln!("Connessione chiusa dal server durante l'autenticazione.");
        return Ok(());
    }

    let auth_resp: Message = serde_json::from_str(&line)?;
    let user_id = match auth_resp {
        Message::AuthResponse { success, user_id, message } if success => {
            println!("Server: {}", message);
            user_id.unwrap()
        }
        _ => {
            eprintln!("Autenticazione fallita.");
            return Ok(());
        }
    };

    // 2. Simula GPS
    let user_id_clone = user_id.clone();
    let (tx_gps, mut rx_gps) = tokio::sync::mpsc::channel::<Message>(10);
    
    tokio::spawn(async move {
        let mut lat = 45.0;
        let mut lon = 7.0;

        loop {
            // Random walk
            lat += rand::thread_rng().gen_range(-0.01..0.01);
            lon += rand::thread_rng().gen_range(-0.01..0.01);
            
            let coords = Coordinates { latitude: lat, longitude: lon };
            let msg = Message::PositionUpdate {
                user_id: user_id_clone.clone(),
                coords,
                timestamp: Utc::now(),
            };
            
            if tx_gps.send(msg).await.is_err() {
                break;
            }
            
            // Pausa tra due posizioni
            // NOTA: Requisiti = 30 sec, metto 30 sec
            tokio::time::sleep(Duration::from_secs(30)).await;
        }
    });

    // 3. Simula console input per chat
    let user_id_cli = user_id.clone();
    let (tx_cli, mut rx_cli) = tokio::sync::mpsc::channel::<Message>(10);
    
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut input = String::new();
        loop {
            input.clear();
            if let Ok(bytes) = reader.read_line(&mut input).await {
                if bytes == 0 { break; }
                let text = input.trim().to_string();
                if !text.is_empty() {
                    let msg = Message::ClientToServerText {
                        user_id: user_id_cli.clone(),
                        content: text,
                    };
                    if tx_cli.send(msg).await.is_err() {
                        break;
                    }
                }
            }
        }
    });

    // Main loop multiplexing output (GPS+CLI) and Input (Server)
    loop {
        line.clear();
        tokio::select! {
            // Leggi dal server
            bytes_read = reader.read_line(&mut line) => {
                let bytes = bytes_read?;
                if bytes == 0 {
                    println!("Disconnesso dal server.");
                    break;
                }
                if let Ok(msg) = serde_json::from_str::<Message>(&line) {
                    match msg {
                        Message::ServerToClientBroadcast { content } => {
                            println!("[BROADCAST]: {}", content);
                        }
                        Message::ServerToClientDirect { target_user_id: _, content } => {
                            println!("[MESSAGGIO]: {}", content);
                        }
                        _ => {}
                    }
                }
            }
            // Manda GPS al server
            Some(msg) = rx_gps.recv() => {
                let json = serde_json::to_string(&msg)? + "\n";
                write_half.write_all(json.as_bytes()).await?;
            }
            // Manda CLI input al server
            Some(msg) = rx_cli.recv() => {
                let json = serde_json::to_string(&msg)? + "\n";
                write_half.write_all(json.as_bytes()).await?;
            }
        }
    }

    Ok(())
}
