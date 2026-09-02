mod analysis;
mod auth;
mod db;

use chrono::{DateTime, Utc};
use shared::{Coordinates, Message, UserState, UserId};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, RwLock};
use auth::{hash_password, verify_password};
use db::{init_db, DbPool};

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
    db_pool: DbPool,
}

//FIXME: rivedi gestione lock
type SharedState = Arc<RwLock<ServerState>>;

use std::fs::OpenOptions;
use std::io::Write;
use sysinfo::{System, SystemExt, ProcessExt, get_current_pid};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_pool = init_db().expect("Impossibile inizializzare il database");
    let state_data = ServerState {
        clients: HashMap::new(),
        db_pool,
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
                        for (uid, client) in r_state.clients.iter() {
                            if client.username == target_name {
                                let _ = client.sender.send(direct_msg.clone()).await;
                                let _ = db::insert_chat(&r_state.db_pool, "Server", Some(uid), msg_content, Utc::now());
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
                        
                        match db::get_user_by_name(&r_state.db_pool, target_name) {
                            Ok(Some((uid, _))) => {
                                match db::get_user_history(&r_state.db_pool, &uid, start_time, end_time) {
                                    Ok((states, distances)) => {
                                        let result = analysis::analyze_movement(&states, &distances, start_time, end_time);
                                        
                                        // Trova lo stato attuale se online
                                        let mut state_str = "Disconnesso";
                                        for (_, client) in r_state.clients.iter() {
                                            if client.username == target_name {
                                                state_str = match client.state {
                                                    UserState::Fermo => "Fermo",
                                                    UserState::InMovimento => "In Movimento",
                                                    UserState::Disconnesso => "Disconnesso",
                                                };
                                                break;
                                            }
                                        }
                                        
                                        println!("=== STATISTICHE per {} ({}) ===\nStato Attuale: {}\nDistanza: {:.2} km\nVelocità Media: {:.2} km/h\nTempo in Movimento: {} sec\nTempo Pause: {} sec\n===============================", 
                                            target_name, interval, state_str, result.total_distance_km, result.average_speed_kmh, result.moving_time_secs, result.pause_time_secs);
                                    }
                                    Err(e) => println!("Errore nel recupero storico: {}", e),
                                }
                            }
                            Ok(None) => println!("Utente {} non trovato nel database.", target_name),
                            Err(e) => println!("Errore DB: {}", e),
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
                } else if text.starts_with("/chat ") {
                    let parts: Vec<&str> = text.split_whitespace().collect();
                    if parts.len() == 2 {
                        let target_name = parts[1];
                        let r_state = state_for_stdin.read().await;
                        
                        match db::get_user_by_name(&r_state.db_pool, target_name) {
                            Ok(Some((uid, _))) => {
                                match db::get_chat_history(&r_state.db_pool, &uid) {
                                    Ok(chats) => {
                                        println!("=== STORICO CHAT con {} ===", target_name);
                                        if chats.is_empty() {
                                            println!("(Nessun messaggio)");
                                        } else {
                                            for (sender, content, ts) in chats {
                                                let display_sender = if sender == "Server" { "Server" } else { target_name };
                                                println!("[{}] {}: {}", ts.format("%Y-%m-%d %H:%M:%S"), display_sender, content);
                                            }
                                        }
                                        println!("===============================");
                                    }
                                    Err(e) => println!("Errore nel recupero chat: {}", e),
                                }
                            }
                            Ok(None) => println!("Utente {} non trovato nel database.", target_name),
                            Err(e) => println!("Errore DB: {}", e),
                        }
                    } else {
                        println!("Uso corretto: /chat <utente>");
                    }
                } else {
                    println!("--- Menu Comandi Server ---");
                    println!("/msg <utente> <testo>  : Invia un messaggio privato a un utente");
                    println!("/b <testo>             : Invia un messaggio broadcast a tutti");
                    println!("/stats <utente> <int.> : Mostra le statistiche (all, giorno, settimana, mese)");
                    println!("/chat <utente>         : Mostra lo storico dei messaggi con un utente");
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
                let bytes_read = match bytes_read {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Errore lettura client: {}", e);
                        break;
                    }
                };
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
                        let w_state = state.write().await;
                        // Hashing password
                        let hashed_password = match hash_password(&password) {
                            Ok(h) => h,
                            Err(e) => {
                                eprintln!("Errore hash password: {}", e);
                                continue;
                            }
                        };
                        let new_id = uuid::Uuid::new_v4().to_string();
                        
                        match db::register_user(&w_state.db_pool, &new_id, &username, &hashed_password) {
                            Ok(true) => {
                                let response = Message::RegisterResponse {
                                    success: true,
                                    message: "Registrazione completata".to_string(),
                                };
                                let response_json = serde_json::to_string(&response)? + "\n";
                                if write_half.write_all(response_json.as_bytes()).await.is_err() {
                                    break;
                                }
                                println!("Nuovo utente registrato: {}", username);
                            }
                            Ok(false) => {
                                let response = Message::RegisterResponse {
                                    success: false,
                                    message: "Utente già esistente".to_string(),
                                };
                                let response_json = serde_json::to_string(&response)? + "\n";
                                if write_half.write_all(response_json.as_bytes()).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Errore DB in registrazione: {}", e);
                            }
                        }
                    },

                    Message::LoginRequest { username, password } => {
                        let mut w_state = state.write().await;
                        
                        // Controllo per impedire il doppio login di utenti attivi
                        let is_already_connected = w_state.clients.values().any(|c| c.username == username && c.state != UserState::Disconnesso);
                        if is_already_connected {
                            let response = Message::LoginResponse {
                                success: false,
                                user_id: None,
                                message: "Utente già connesso su un altro dispositivo".to_string(),
                            };
                            let response_json = serde_json::to_string(&response)? + "\n";
                            if write_half.write_all(response_json.as_bytes()).await.is_err() {
                                break;
                            }
                            continue;
                        }

                        match db::get_user_by_name(&w_state.db_pool, &username) {
                            Ok(Some((db_id, db_hash))) => {
                                if verify_password(&password, &db_hash) {
                                    current_user_id = Some(db_id.clone());
                                    let now = Utc::now();
                                    
                                    let client_data = ClientData {
                                        username: username.clone(),
                                        state: UserState::Fermo, 
                                        last_position: None,
                                        last_move_time: None,
                                        state_history: vec![(UserState::Fermo, now)],
                                        distance_history: Vec::new(),
                                        sender: tx.clone(),
                                    };
                                    
                                    let _ = db::insert_state(&w_state.db_pool, &db_id, "Fermo", now);
                                    w_state.clients.insert(db_id.clone(), client_data);
                                    
                                    let response = Message::LoginResponse {
                                        success: true,
                                        user_id: Some(db_id.clone()),
                                        message: format!("Benvenuto {}", username),
                                    };
                                    let response_json = serde_json::to_string(&response)? + "\n";
                                    if write_half.write_all(response_json.as_bytes()).await.is_err() {
                                        break;
                                    }
                                    println!("Utente {} autenticato con ID {}", username, db_id);
                                } else {
                                    let response = Message::LoginResponse {
                                        success: false,
                                        user_id: None,
                                        message: "Password errata".to_string(),
                                    };
                                    let response_json = serde_json::to_string(&response)? + "\n";
                                    if write_half.write_all(response_json.as_bytes()).await.is_err() {
                                        break;
                                    }
                                }
                            }
                            Ok(None) => {
                                let response = Message::LoginResponse {
                                    success: false,
                                    user_id: None,
                                    message: "Utente non trovato".to_string(),
                                };
                                let response_json = serde_json::to_string(&response)? + "\n";
                                if write_half.write_all(response_json.as_bytes()).await.is_err() {
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Errore DB in login: {}", e);
                            }
                        }
                    },
                    Message::PositionUpdate { user_id, coords, timestamp } => {
                        if Some(user_id.clone()) == current_user_id {
                            let mut w_state = state.write().await;
                            let pool = w_state.db_pool.clone();
                            if let Some(client) = w_state.clients.get_mut(&user_id) {
                                if let Some(last_pos) = &client.last_position {
                                    let dist = crate::analysis::calculate_distance(last_pos, &coords);
                                    
                                    client.distance_history.push((dist, timestamp));
                                    let _ = db::insert_distance(&pool, &user_id, dist, timestamp);
                                    
                                    if dist > 0.001 {
                                        if client.state != UserState::InMovimento {
                                            client.state = UserState::InMovimento;
                                            client.state_history.push((UserState::InMovimento, timestamp));
                                            let _ = db::insert_state(&pool, &user_id, "In Movimento", timestamp);
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
                        let w_state = state.write().await;
                        let sender_name = w_state.clients.get(&user_id).map(|c| c.username.clone()).unwrap_or_else(|| user_id.clone());

                        let _ = db::insert_chat(&w_state.db_pool, &user_id, Some("Server"), &content, Utc::now());
                        
                        // Messaggio diretto al server
                        println!("Messaggio da {}: {}", sender_name, content);
                    },
                    _ => {}
                }
            }
            Some(out_msg) = rx.recv() => {
                let json = serde_json::to_string(&out_msg)? + "\n";
                if write_half.write_all(json.as_bytes()).await.is_err() {
                    break;
                }
            }
        }
    }

    if let Some(user_id) = current_user_id {
        let mut w_state = state.write().await;
        if let Some(client) = w_state.clients.get_mut(&user_id) {
            client.state = UserState::Disconnesso;
            let now = Utc::now();
            client.state_history.push((UserState::Disconnesso, now));
            let _ = db::insert_state(&w_state.db_pool, &user_id, "Disconnesso", now);
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
        let pool = w_state.db_pool.clone();
        for (user_id, client) in w_state.clients.iter_mut() {
            if client.state == UserState::InMovimento {
                if let Some(last_time) = client.last_move_time {
                    // Se non ci sono aggiornamenti di movimento per 3 minuti
                    if now.signed_duration_since(last_time).num_minutes() >= 3 {
                        client.state = UserState::Fermo;
                        client.state_history.push((UserState::Fermo, now));
                        let _ = db::insert_state(&pool, user_id, "Fermo", now);
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
