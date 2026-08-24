use chrono::Utc;
use rand::Rng;
use shared::{Coordinates, Message};
use std::io::{self, Write};
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (user_id, mut write_half, mut reader) = loop {
        println!("=== GeoRuggine ===");
        println!("1) Login");
        println!("2) Registrazione");
        print!("Scelta: ");
        io::stdout().flush().unwrap();
        
        let mut scelta = String::new();
        io::stdin().read_line(&mut scelta).unwrap();
        
        print!("Username: ");
        io::stdout().flush().unwrap();
        let mut username = String::new();
        io::stdin().read_line(&mut username).unwrap();
        let username = username.trim().to_string();

        print!("Password: ");
        io::stdout().flush().unwrap();
        let mut password = String::new();
        io::stdin().read_line(&mut password).unwrap();
        let password = password.trim().to_string();

        println!("Avvio client come {}", username);
        
        let stream = match TcpStream::connect("127.0.0.1:8080").await {
            Ok(s) => s,
            Err(e) => {
                eprintln!("Impossibile connettersi al server: {}. Riprova.\n", e);
                continue;
            }
        };
        println!("Connesso al server.");

        // 1. Auth Phase
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);
        let mut line = String::new();

        if scelta.trim() == "2" {
            let reg_msg = Message::RegisterRequest { username: username.clone(), password: password.clone() };
            let json_reg = serde_json::to_string(&reg_msg).unwrap() + "\n";
            let _ = write_half.write_all(json_reg.as_bytes()).await;
            
            let bytes_read = reader.read_line(&mut line).await.unwrap_or(0);
            if bytes_read == 0 {
                eprintln!("Connessione chiusa.\n");
                continue;
            }
            
            if let Ok(reg_resp) = serde_json::from_str::<Message>(&line) {
                if let Message::RegisterResponse { success, message } = reg_resp {
                    println!("Server: {}", message);
                    if !success { 
                        println!("Riprova.\n");
                        continue; 
                    }
                }
            }
            line.clear();
        }

        let login_msg = Message::LoginRequest { username, password };
        let json_login = serde_json::to_string(&login_msg).unwrap() + "\n";
        let _ = write_half.write_all(json_login.as_bytes()).await;

        let bytes_read = reader.read_line(&mut line).await.unwrap_or(0);
        if bytes_read == 0 {
            eprintln!("Connessione chiusa dal server durante l'autenticazione.\n");
            continue;
        }

        if let Ok(auth_resp) = serde_json::from_str::<Message>(&line) {
            match auth_resp {
                Message::LoginResponse { success, user_id, message } if success => {
                    println!("Server: {}", message);
                    break (user_id.unwrap(), write_half, reader);
                }
                Message::LoginResponse { message, .. } => {
                    eprintln!("Errore di login: {}\nRiprova.\n", message);
                    continue;
                }
                _ => {
                    eprintln!("Risposta inattesa. Riprova.\n");
                    continue;
                }
            }
        }
    };

    // 2. Simula GPS
    let user_id_clone = user_id.clone();
    let (tx_gps, mut rx_gps) = tokio::sync::mpsc::channel::<Message>(10);
    
    tokio::spawn(async move {
        let mut lat = 45.0;
        let mut lon = 7.0;
        let mut pause_counter = 0;

        loop {
            if pause_counter > 0 {
                pause_counter -= 1;
                // La posizione rimane immutata, simulando una sosta
            } else {
                // 15% di probabilità di fermarsi (sosta casuale)
                let rng: f64 = rand::thread_rng().gen();
                if rng < 0.15 {
                    // Imposta la pausa per 8 cicli (8 * 30s = 4 minuti)
                    // In questo modo, dopo 3 minuti esatti il server lo segnalerà come Fermo
                    pause_counter = 8;
                } else {
                    // Random walk
                    lat += rand::thread_rng().gen_range(-0.01..0.01);
                    lon += rand::thread_rng().gen_range(-0.01..0.01);
                }
            }
            
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
                if text.is_empty() { continue; }

                if text.starts_with("/msg ") {
                    let msg_content = text.strip_prefix("/msg ").unwrap().trim();
                    let msg = Message::ClientToServerText {
                        user_id: user_id_cli.clone(),
                        content: msg_content.to_string(),
                    };
                    if tx_cli.send(msg).await.is_err() { break; }
                } else {
                    println!("Sistema: Usa il comando /msg <testo> per inviare un messaggio al server");
                }
            }
        }
    });

    // Main loop multiplexing output (GPS+CLI) and Input (Server)
    let mut line = String::new();
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
