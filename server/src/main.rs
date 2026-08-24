use chrono::{DateTime, Utc};
use shared::{Coordinates, Message, UserState, UserId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};

mod analysis;

#[derive(Debug, Clone)]
struct ClientData {
    username: String,
    state: UserState,
    last_position: Option<Coordinates>,
    last_update_time: Option<DateTime<Utc>>,
    position_history: Vec<(Coordinates, DateTime<Utc>)>,
    sender: mpsc::Sender<Message>,
}

struct ServerState {
    clients: HashMap<UserId, ClientData>,
    accounts: HashMap<String, String>, // username -> password
}

type SharedState = Arc<RwLock<ServerState>>;

use std::fs::{self, OpenOptions};
use std::io::Write;
use sysinfo::{System, SystemExt, CpuExt};

fn load_accounts() -> HashMap<String, String> {
    if let Ok(content) = fs::read_to_string("accounts.json") {
        if let Ok(accounts) = serde_json::from_str(&content) {
            return accounts;
        }
    }
    HashMap::new()
}

fn save_accounts(accounts: &HashMap<String, String>) {
    if let Ok(content) = serde_json::to_string_pretty(accounts) {
        let _ = fs::write("accounts.json", content);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state_data = ServerState {
        clients: HashMap::new(),
        accounts: load_accounts(),
    };
    let state: SharedState = Arc::new(RwLock::new(state_data));
    let listener = TcpListener::bind("127.0.0.1:8080").await?;
    println!("Server in ascolto su 127.0.0.1:8080");

    let state_clone = state.clone();
    tokio::spawn(async move {
        state_monitor_task(state_clone).await;
    });

    tokio::spawn(async move {
        cpu_logger_task().await;
    });

    loop {
        let (socket, _) = listener.accept().await?;
        let state_clone = state.clone();
        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, state_clone).await {
                eprintln!("Errore client: {}", e);
            }
        });
    }
}

async fn handle_client(mut socket: TcpStream, state: SharedState) -> Result<(), Box<dyn std::error::Error>> {
    let (read_half, mut write_half) = socket.split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    // 1. Auth Phase
    let mut current_user_id: Option<UserId> = None;
    let (tx, mut rx) = mpsc::channel::<Message>(32);

    loop {
        line.clear();
        tokio::select! {
            bytes_read = reader.read_line(&mut line) => {
                let bytes_read = bytes_read?;
                if bytes_read == 0 {
                    break; // EOF
                }
                
                let msg: Message = match serde_json::from_str(&line) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("Errore parsing JSON: {}", e);
                        continue;
                    }
                };

                match msg {
                    Message::RegisterRequest { username, password } => {
                        let mut w_state = state.write().await;
                        if w_state.accounts.contains_key(&username) {
                            let response = Message::RegisterResponse {
                                success: false,
                                message: "Utente già esistente".to_string(),
                            };
                            let response_json = serde_json::to_string(&response)? + "\n";
                            write_half.write_all(response_json.as_bytes()).await?;
                        } else {
                            w_state.accounts.insert(username.clone(), password);
                            save_accounts(&w_state.accounts);
                            let response = Message::RegisterResponse {
                                success: true,
                                message: "Registrazione completata".to_string(),
                            };
                            let response_json = serde_json::to_string(&response)? + "\n";
                            write_half.write_all(response_json.as_bytes()).await?;
                            println!("Nuovo utente registrato: {}", username);
                        }
                    },
                    Message::LoginRequest { username, password } => {
                        let mut w_state = state.write().await;
                        if let Some(stored_pwd) = w_state.accounts.get(&username) {
                            if stored_pwd == &password {
                                let user_id = uuid::Uuid::new_v4().to_string();
                                current_user_id = Some(user_id.clone());
                                
                                let client_data = ClientData {
                                    username: username.clone(),
                                    state: UserState::Fermo, 
                                    last_position: None,
                                    last_update_time: None,
                                    position_history: Vec::new(),
                                    sender: tx.clone(),
                                };
                                
                                w_state.clients.insert(user_id.clone(), client_data);
                                
                                let response = Message::LoginResponse {
                                    success: true,
                                    user_id: Some(user_id.clone()),
                                    message: format!("Benvenuto {}", username),
                                };
                                let response_json = serde_json::to_string(&response)? + "\n";
                                write_half.write_all(response_json.as_bytes()).await?;
                                println!("Utente {} autenticato con ID {}", username, user_id);
                            } else {
                                let response = Message::LoginResponse {
                                    success: false,
                                    user_id: None,
                                    message: "Password errata".to_string(),
                                };
                                let response_json = serde_json::to_string(&response)? + "\n";
                                write_half.write_all(response_json.as_bytes()).await?;
                            }
                        } else {
                            let response = Message::LoginResponse {
                                success: false,
                                user_id: None,
                                message: "Utente non trovato".to_string(),
                            };
                            let response_json = serde_json::to_string(&response)? + "\n";
                            write_half.write_all(response_json.as_bytes()).await?;
                        }
                    },
                    Message::PositionUpdate { user_id, coords, timestamp } => {
                        if Some(user_id.clone()) == current_user_id {
                            let mut w_state = state.write().await;
                            if let Some(client) = w_state.clients.get_mut(&user_id) {
                                // Verifica transizione di stato
                                if let Some(last_pos) = &client.last_position {
                                    if last_pos != &coords && client.state != UserState::InMovimento {
                                        client.state = UserState::InMovimento;
                                        println!("Utente {} è ora in stato: In Movimento", client.username);
                                    }
                                } else {
                                    client.state = UserState::Fermo; // prima posizione
                                    println!("Utente {} è ora in stato: Fermo", client.username);
                                }
                                
                                client.last_position = Some(coords.clone());
                                client.last_update_time = Some(timestamp);
                                client.position_history.push((coords, timestamp));
                            }
                        }
                    },
                    Message::ClientToServerText { user_id, content } => {
                        let sender_name = {
                            let r_state = state.read().await;
                            r_state.clients.get(&user_id).map(|c| c.username.clone()).unwrap_or_else(|| user_id.clone())
                        };

                        if content.starts_with("/stats") {
                            let parts: Vec<&str> = content.split_whitespace().collect();
                            let interval = if parts.len() > 1 { parts[1] } else { "all" };
                            
                            let end_time = Utc::now();
                            let start_time = match interval {
                                "giorno" => end_time - chrono::Duration::days(1),
                                "settimana" => end_time - chrono::Duration::weeks(1),
                                "mese" => end_time - chrono::Duration::days(30),
                                _ => chrono::DateTime::<Utc>::MIN_UTC, // all
                            };

                            let r_state = state.read().await;
                            if let Some(client) = r_state.clients.get(&user_id) {
                                let result = analysis::analyze_movement(&client.position_history, start_time, end_time);
                                let state_str = match client.state {
                                    UserState::Fermo => "Fermo",
                                    UserState::InMovimento => "In Movimento",
                                    UserState::Disconnected => "Sconnesso",
                                };
                                let stats_msg = format!("=== STATISTICHE ({}) ===\nStato Attuale: {}\nDistanza: {:.2} km\nVelocità Media: {:.2} km/h\nTempo in Movimento: {} sec\nTempo Pause: {} sec\n===================", 
                                    interval, state_str, result.total_distance_km, result.average_speed_kmh, result.moving_time_secs, result.pause_time_secs);
                                let _ = tx.send(Message::ServerToClientDirect { target_user_id: user_id.clone(), content: stats_msg }).await;
                            }
                        } else if content.starts_with("/msg ") {
                            let parts: Vec<&str> = content.splitn(3, ' ').collect();
                            if parts.len() == 3 {
                                let target_name = parts[1];
                                let msg_text = parts[2];
                                
                                let r_state = state.read().await;
                                let mut found = false;
                                for (target_id, target_client) in r_state.clients.iter() {
                                    if target_client.username == target_name {
                                        found = true;
                                        let direct_msg = Message::ServerToClientDirect {
                                            target_user_id: target_id.clone(),
                                            content: format!("[Messaggio Privato da {}]: {}", sender_name, msg_text),
                                        };
                                        let _ = target_client.sender.send(direct_msg).await;
                                    }
                                }
                                if !found {
                                    let _ = tx.send(Message::ServerToClientDirect {
                                        target_user_id: user_id.clone(),
                                        content: format!("Sistema: Utente '{}' non trovato.", target_name),
                                    }).await;
                                }
                            } else {
                                let _ = tx.send(Message::ServerToClientDirect {
                                    target_user_id: user_id.clone(),
                                    content: "Sistema: Formato non valido. Usa: /msg <nome> <messaggio>".to_string(),
                                }).await;
                            }
                        } else {
                            // Broadcast
                            let broadcast_msg = Message::ServerToClientBroadcast {
                                content: format!("[{}] dice: {}", sender_name, content),
                            };
                            let r_state = state.read().await;
                            for (target_id, target_client) in r_state.clients.iter() {
                                // Opzionale: Non inviare a noi stessi
                                if target_id != &user_id {
                                    let _ = target_client.sender.send(broadcast_msg.clone()).await;
                                }
                            }
                        }
                    },
                    _ => {}
                }
            }
            Some(out_msg) = rx.recv() => {
                let json = serde_json::to_string(&out_msg)? + "\n";
                write_half.write_all(json.as_bytes()).await?;
            }
        }
    }

    if let Some(user_id) = current_user_id {
        let mut w_state = state.write().await;
        if let Some(client) = w_state.clients.get_mut(&user_id) {
            client.state = UserState::Disconnected;
        }
        println!("Utente {} disconnesso", user_id);
    }

    Ok(())
}

async fn state_monitor_task(state: SharedState) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));
    loop {
        interval.tick().await;
        let now = Utc::now();
        let mut w_state = state.write().await;
        for (_, client) in w_state.clients.iter_mut() {
            if client.state == UserState::InMovimento {
                if let Some(last_time) = client.last_update_time {
                    // Se non ci sono aggiornamenti per 3 minuti
                    if now.signed_duration_since(last_time).num_minutes() >= 3 {
                        client.state = UserState::Fermo;
                        println!("Utente {} passato a stato Fermo per inattività", client.username);
                    }
                }
            }
        }
    }
}

async fn cpu_logger_task() {
    let mut sys = System::new_all();
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(120)); // Ogni 2 minuti
    
    // Assicuriamoci di fare il primo refresh prima del loop
    sys.refresh_cpu();

    loop {
        interval.tick().await;
        sys.refresh_cpu();
        let cpu_usage: f32 = sys.cpus().iter().map(|cpu| cpu.cpu_usage()).sum::<f32>() / sys.cpus().len() as f32;
        
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("cpu_log.txt") 
        {
            let log_line = format!("[{}] CPU Usage: {:.2}%\n", Utc::now(), cpu_usage);
            let _ = file.write_all(log_line.as_bytes());
        }
    }
}
