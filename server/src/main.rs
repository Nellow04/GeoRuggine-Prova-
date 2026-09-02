mod analysis;
mod auth;

use chrono::{DateTime, Utc};
use shared::{Coordinates, Message, UserState, UserId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use auth::{load_accounts, hash_password,verify_password,save_accounts};


#[derive(Debug, Clone)]
struct ClientData {
    username: String,
    state: UserState,
    last_position: Option<Coordinates>,
    last_move_time: Option<DateTime<Utc>>,
    state_history: Vec<(UserState, DateTime<Utc>)>,
    distance_history: Vec<(f64, DateTime<Utc>)>,
    sender: mpsc::Sender<Message>,
}

struct ServerState {
    clients: HashMap<UserId, ClientData>,
    accounts: HashMap<String, String>, // username -> password hash
}

//FIXME: rivedi gestione lock
type SharedState = Arc<RwLock<ServerState>>;

use std::fs::OpenOptions;
use std::io::Write;
use sysinfo::{System, SystemExt, ProcessExt, get_current_pid};

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

    let state_for_stdin = state.clone();
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = tokio::io::BufReader::new(stdin);
        let mut input = String::new();
        use tokio::io::AsyncBufReadExt;
        
        loop {
            input.clear();
            if let Ok(bytes) = reader.read_line(&mut input).await {
                if bytes == 0 { break; }
                let text = input.trim();
                if text.is_empty() { continue; }

                if text.starts_with("/msg ") {
                    let parts: Vec<&str> = text.splitn(3, ' ').collect();
                    if parts.len() == 3 {
                        let target_name = parts[1];
                        let msg_content = parts[2];
                        let r_state = state_for_stdin.read().await;
                        //FIXME: cambia nome del campo userid
                        let direct_msg = Message::ServerToClientDirect { 
                            target_user_id: "Server".to_string(), 
                            content: msg_content.to_string() 
                        };
                        let mut found = false;
                        for (_, client) in r_state.clients.iter() {
                            if client.username == target_name {
                                let _ = client.sender.send(direct_msg.clone()).await;
                                found = true;
                            }
                        }
                        if found {
                            println!("Messaggio privato inviato a {}", target_name);
                        } else {
                            println!("Utente {} non trovato", target_name);
                        }
                    } else {
                        println!("Uso corretto: /msg <utente> <messaggio>");
                    }
                } else if text.starts_with("/stats") {
                    let parts: Vec<&str> = text.split_whitespace().collect();
                    if parts.len() == 3 {
                        let target_name = parts[1];
                        let interval = parts[2];

                        if interval != "giorno" && interval != "settimana" && interval != "mese" && interval != "all" {
                            println!("Intervallo '{}' non valido. Usa: giorno, settimana, mese, all", interval);
                            continue;
                        }
                        
                        let end_time = chrono::Utc::now();
                        let start_time = match interval {
                            "giorno" => {
                                use chrono::{Datelike, TimeZone};
                                chrono::Utc.with_ymd_and_hms(end_time.year(), end_time.month(), end_time.day(), 0, 0, 0).unwrap()
                            },
                            "settimana" => {
                                use chrono::{Datelike, TimeZone};
                                let days_from_monday = end_time.weekday().num_days_from_monday();
                                let monday = end_time - chrono::Duration::days(days_from_monday as i64);
                                chrono::Utc.with_ymd_and_hms(monday.year(), monday.month(), monday.day(), 0, 0, 0).unwrap()
                            },
                            "mese" => {
                                use chrono::{Datelike, TimeZone};
                                chrono::Utc.with_ymd_and_hms(end_time.year(), end_time.month(), 1, 0, 0, 0).unwrap()
                            },
                            _ => chrono::DateTime::<Utc>::MIN_UTC,
                        };

                        let r_state = state_for_stdin.read().await;
                        let mut found = false;
                        for (_, client) in r_state.clients.iter() {
                            if client.username == target_name {
                                found = true;
                                let result = analysis::analyze_movement(&client.state_history, &client.distance_history, start_time, end_time);
                                let state_str = match client.state {
                                    UserState::Fermo => "Fermo",
                                    UserState::InMovimento => "In Movimento",
                                    UserState::Disconnected => "Sconnesso",
                                };
                                println!("=== STATISTICHE per {} ({}) ===\nStato Attuale: {}\nDistanza: {:.2} km\nVelocità Media: {:.2} km/h\nTempo in Movimento: {} sec\nTempo Pause: {} sec\n===============================", 
                                    target_name, interval, state_str, result.total_distance_km, result.average_speed_kmh, result.moving_time_secs, result.pause_time_secs);
                                break;
                            }
                        }
                        if !found {
                            println!("Utente {} non trovato tra gli utenti correnti.", target_name);
                        }
                    } else {
                        println!("Uso corretto: /stats <utente> <giorno|settimana|mese|all>");
                    }
                } else if text.starts_with("/b ") {
                    let msg_content = text.strip_prefix("/b ").unwrap().trim();
                    let broadcast_msg = Message::ServerToClientBroadcast { 
                        content: msg_content.to_string() 
                    };
                    let r_state = state_for_stdin.read().await;
                    for (_, client) in r_state.clients.iter() {
                        let _ = client.sender.send(broadcast_msg.clone()).await;
                    }
                    println!("Messaggio broadcast inviato a tutti");
                } else {
                    println!("--- Menu Comandi Server ---");
                    println!("/msg <utente> <testo>  : Invia un messaggio privato a un utente");
                    println!("/b <testo>             : Invia un messaggio broadcast a tutti");
                    println!("/stats <utente> <int.> : Mostra le statistiche (all, giorno, settimana, mese)");
                    println!("---------------------------");
                }
            }
        }
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

                // TODO [DEBUG]: Stampa di debug per visualizzare tutti i segnali JSON in arrivo (da rimuovere in produzione)
                println!("[DEBUG RICEVUTO]: {}", line.trim());
                
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
                            //fixme: hash password
                            let hashed_password = hash_password(&password)?;
                            w_state.accounts.insert(username.clone(), hashed_password);
                            save_accounts(&w_state.accounts)? ;
                            let response = Message::RegisterResponse {
                                success: true,
                                message: "Registrazione completata".to_string(),
                            };
                            let response_json = serde_json::to_string(&response)? + "\n";
                            write_half.write_all(response_json.as_bytes()).await?;
                            println!("Nuovo utente registrato: {}", username);
                        }
                    },

                    //FIXME: password salvate in chiaro
                    Message::LoginRequest { username, password } => {
                        let mut w_state = state.write().await;
                        
                        // Controllo per impedire il doppio login
                        let is_already_connected = w_state.clients.values().any(|c| c.username == username && c.state != UserState::Disconnected);
                        if is_already_connected {
                            let response = Message::LoginResponse {
                                success: false,
                                user_id: None,
                                message: "Utente già connesso su un altro dispositivo".to_string(),
                            };
                            let response_json = serde_json::to_string(&response)? + "\n";
                            write_half.write_all(response_json.as_bytes()).await?;
                            continue;
                        }

                        if let Some(stored_pwd) = w_state.accounts.get(&username) {
                            //TODO: controlla hash e salt della password
                            if verify_password(&password,stored_pwd,) {
                                let user_id = uuid::Uuid::new_v4().to_string();
                                current_user_id = Some(user_id.clone());
                                
                                let client_data = ClientData {
                                    username: username.clone(),
                                    state: UserState::Fermo, 
                                    last_position: None,
                                    last_move_time: None,
                                    state_history: vec![(UserState::Fermo, Utc::now())],
                                    distance_history: Vec::new(),
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
                                if let Some(last_pos) = &client.last_position {
                                    let dist = crate::analysis::calculate_distance(last_pos, &coords);
                                    
                                    client.distance_history.push((dist, timestamp));
                                    
                                    if dist > 0.001 {
                                        if client.state != UserState::InMovimento {
                                            client.state = UserState::InMovimento;
                                            client.state_history.push((UserState::InMovimento, timestamp));
                                            println!("Utente {} è ora in stato: In Movimento", client.username);
                                        }
                                        client.last_move_time = Some(timestamp);
                                    }
                                } else {
                                    // prima posizione
                                    client.last_move_time = Some(timestamp);
                                }
                                
                                client.last_position = Some(coords.clone());
                            }
                        }
                    },
                    Message::ClientToServerText { user_id, content } => {
                        let sender_name = {
                            let r_state = state.read().await;
                            r_state.clients.get(&user_id).map(|c| c.username.clone()).unwrap_or_else(|| user_id.clone())
                        };

                        // Messaggio diretto al server
                        println!("Messaggio da {}: {}", sender_name, content);
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
            client.state_history.push((UserState::Disconnected, Utc::now()));
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
                if let Some(last_time) = client.last_move_time {
                    // Se non ci sono aggiornamenti di movimento per 3 minuti
                    if now.signed_duration_since(last_time).num_minutes() >= 3 {
                        client.state = UserState::Fermo;
                        client.state_history.push((UserState::Fermo, now));
                        println!("Utente {} passato a stato Fermo per inattività", client.username);
                    }
                }
            }
        }
    }
}


//FIXME: logga solo la cpu del server: Io userei il PID del processo corrente e sysinfo per leggere process.cpu_usage()
async fn cpu_logger_task() {
    let mut sys = System::new();
    let pid = get_current_pid().unwrap();
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(120)); // Ogni 2 minuti
    
    // Primo refresh per inizializzare il calcolo per questo processo
    sys.refresh_process(pid);

    loop {
        interval.tick().await;
        sys.refresh_process(pid);
        
        let cpu_usage = if let Some(process) = sys.process(pid) {
            process.cpu_usage()
        } else {
            0.0
        };
        
        if let Ok(mut file) = OpenOptions::new()
            .create(true)
            .append(true)
            .open("cpu_log.txt") 
        {
            let log_line = format!("[{}] Server Process CPU Usage: {:.2}%\n", Utc::now(), cpu_usage);
            let _ = file.write_all(log_line.as_bytes());
        }
    }
}
