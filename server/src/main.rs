mod analysis;
mod auth;
mod db;

use auth::{hash_password, load_accounts, save_accounts, verify_password};

use chrono::{DateTime, Utc};

use shared::{Coordinates, Message, UserId, UserState};

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use tokio::net::{TcpListener, TcpStream};

use tokio::sync::{mpsc, RwLock};
use auth::{hash_password, verify_password};
use db::{init_db, DbPool};

use std::fs::OpenOptions;
use std::io::Write;

use sysinfo::{get_current_pid, ProcessExt, System, SystemExt};

// ============================================================
// DATI CLIENT
// ============================================================

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

#[derive(Debug, Clone, Default)]
struct UserHistory {
    state_history: Vec<(UserState, DateTime<Utc>)>,
    distance_history: Vec<(f64, DateTime<Utc>)>,
}

// ============================================================
// STATO SERVER
// ============================================================

struct ServerState {
    clients: HashMap<UserId, ClientData>,
    db_pool: DbPool,
}

// FIXME: rivedi gestione lock
type SharedState = Arc<RwLock<ServerState>>;

// ============================================================
// MAIN
// ============================================================

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

    // ========================================================
    // TASK MONITORAGGIO STATO UTENTI
    // ========================================================

    let state_clone = state.clone();

    tokio::spawn(async move {
        state_monitor_task(state_clone).await;
    });

    // ========================================================
    // TASK INPUT CONSOLE SERVER
    // ========================================================

    let state_for_stdin = state.clone();

    tokio::spawn(async move {
        let stdin = tokio::io::stdin();

        let mut reader = tokio::io::BufReader::new(stdin);

        let mut input = String::new();

        loop {
            input.clear();

            if let Ok(bytes) = reader.read_line(&mut input).await {
                if bytes == 0 {
                    break;
                }

                let text = input.trim();

                if text.is_empty() {
                    continue;
                }

                // =================================================
                // MESSAGGIO PRIVATO
                // =================================================

                if text.starts_with("/msg ") {
                    let parts: Vec<&str> = text.splitn(3, ' ').collect();

                    if parts.len() == 3 {
                        let target_name = parts[1];

                        let msg_content = parts[2];

                        let r_state = state_for_stdin.read().await;

                        // FIXME: cambia nome del campo userid
                        let direct_msg = Message::ServerToClientDirect {
                            target_user_id: "Server".to_string(),

                            content: msg_content.to_string(),
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
                }
                // =================================================
                // STATISTICHE
                // =================================================
                else if text.starts_with("/stats") {
                    let parts: Vec<&str> = text.split_whitespace().collect();

                    if parts.len() == 3 {
                        let target_name = parts[1];

                        let interval = parts[2];

                        if interval != "giorno"
                            && interval != "settimana"
                            && interval != "mese"
                            && interval != "all"
                        {
                            println!(
                                "Intervallo '{}' non valido. \
                                 Usa: giorno, settimana, mese, all",
                                interval
                            );

                            continue;
                        }

                        let end_time = chrono::Utc::now();

                        let start_time = match interval {
                            "giorno" => {
                                use chrono::{Datelike, TimeZone};

                                chrono::Utc
                                    .with_ymd_and_hms(
                                        end_time.year(),
                                        end_time.month(),
                                        end_time.day(),
                                        0,
                                        0,
                                        0,
                                    )
                                    .unwrap()
                            }

                            "settimana" => {
                                use chrono::{Datelike, TimeZone};

                                let days_from_monday = end_time.weekday().num_days_from_monday();

                                let monday =
                                    end_time - chrono::Duration::days(days_from_monday as i64);

                                chrono::Utc
                                    .with_ymd_and_hms(
                                        monday.year(),
                                        monday.month(),
                                        monday.day(),
                                        0,
                                        0,
                                        0,
                                    )
                                    .unwrap()
                            }

                            "mese" => {
                                use chrono::{Datelike, TimeZone};

                                chrono::Utc
                                    .with_ymd_and_hms(end_time.year(), end_time.month(), 1, 0, 0, 0)
                                    .unwrap()
                            }

                            _ => chrono::DateTime::<Utc>::MIN_UTC,
                        };

                        // ricerca dello storico

                        let r_state = state_for_stdin.read().await;
                        
                        match db::get_user_by_name(&r_state.db_pool, target_name) {
                            Ok(Some((uid, _))) => {
                                match db::get_user_history(&r_state.db_pool, &uid, start_time, end_time) {
                                    Ok((states, distances)) => {
                                        let result = analysis::analyze_movement(&states, &distances, start_time, end_time);
                                        
                                        // Trova lo stato attuale se online
                                        let mut state_str = "Sconnesso";
                                        for (_, client) in r_state.clients.iter() {
                                            if client.username == target_name {
                                                state_str = match client.state {
                                                    UserState::Fermo => "Fermo",
                                                    UserState::InMovimento => "In Movimento",
                                                    UserState::Disconnected => "Sconnesso",
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
                        println!(
                            "Uso corretto: \
                             /stats <utente> \
                             <giorno|settimana|mese|all>"
                        );
                    }
                }
                // =================================================
                // BROADCAST
                // =================================================
                else if text.starts_with("/b ") {
                    let msg_content = text.strip_prefix("/b ").unwrap().trim();

                    let broadcast_msg = Message::ServerToClientBroadcast {
                        content: msg_content.to_string(),
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

    // ========================================================
    // TASK LOGGER CPU
    // ========================================================

    tokio::spawn(async move {
        cpu_logger_task().await;
    });

    // ========================================================
    // ACCETTAZIONE CLIENT
    // ========================================================

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

// ============================================================
// GESTIONE CLIENT
// ============================================================

async fn handle_client(
    mut socket: TcpStream,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error>> {
    let (read_half, mut write_half) = socket.split();

    let mut reader = BufReader::new(read_half);

    let mut line = String::new();

    /*
     * Contiene lo user_id dell'utente
     * autenticato su questa connessione.
     *
     * None = nessun utente autenticato.
     */
    let mut current_user_id: Option<UserId> = None;

    /*
     * Channel utilizzato dal server
     * per inviare messaggi al client.
     */
    let (tx, mut rx) = mpsc::channel::<Message>(32);

    // ========================================================
    // LOOP DELLA CONNESSIONE
    // ========================================================

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
                        let w_state = state.write().await;
                        // Hashing password
                        let hashed_password = hash_password(&password)?;
                        let new_id = uuid::Uuid::new_v4().to_string();
                        
                        match db::register_user(&w_state.db_pool, &new_id, &username, &hashed_password) {
                            Ok(true) => {
                                let response = Message::RegisterResponse {
                                    success: true,
                                    message: "Registrazione completata".to_string(),
                                };
                                let response_json = serde_json::to_string(&response)? + "\n";
                                write_half.write_all(response_json.as_bytes()).await?;
                                println!("Nuovo utente registrato: {}", username);
                            }
                            Ok(false) => {
                                let response = Message::RegisterResponse {
                                    success: false,
                                    message: "Utente già esistente".to_string(),
                                };
                                let response_json = serde_json::to_string(&response)? + "\n";
                                write_half.write_all(response_json.as_bytes()).await?;
                            }
                            Err(e) => {
                                eprintln!("Errore DB in registrazione: {}", e);
                            }
                        }
                    },

                    Message::LoginRequest { username, password } => {
                        let mut w_state = state.write().await;
                        
                        // Controllo per impedire il doppio login
                        let is_already_connected = w_state.clients.values().any(|c| c.username == username);
                        if is_already_connected {
                            let response = Message::LoginResponse {
                                success: false,
                                user_id: None,
                                message: "Utente già connesso su un altro dispositivo".to_string(),
                            };

                        match db::get_user_by_name(&w_state.db_pool, &username) {
                            Ok(Some((db_id, db_hash))) => {
                                if verify_password(&password, &db_hash) {
                                    current_user_id = Some(db_id.clone());
                                    
                                    let client_data = ClientData {
                                        username: username.clone(),
                                        state: UserState::Fermo, 
                                        last_position: None,
                                        last_move_time: None,
                                        state_history: vec![(UserState::Fermo, Utc::now())],
                                        distance_history: Vec::new(),
                                        sender: tx.clone(),
                                    };
                                    
                                    w_state.clients.insert(db_id.clone(), client_data);
                                    
                                    let response = Message::LoginResponse {
                                        success: true,
                                        user_id: Some(db_id.clone()),
                                        message: format!("Benvenuto {}", username),
                                    };
                                    let response_json = serde_json::to_string(&response)? + "\n";
                                    write_half.write_all(response_json.as_bytes()).await?;
                                    println!("Utente {} autenticato con ID {}", username, db_id);
                                } else {
                                    let response = Message::LoginResponse {
                                        success: false,
                                        user_id: None,
                                        message: "Password errata".to_string(),
                                    };
                                    let response_json = serde_json::to_string(&response)? + "\n";
                                    write_half.write_all(response_json.as_bytes()).await?;
                                }
                            }
                            Ok(None) => {
                                let response = Message::LoginResponse {
                                    success: false,
                                    user_id: None,
                                    message: "Utente non trovato".to_string(),
                                };


                                println!(
                                    "Messaggio da {}: {}",
                                    sender_name,
                                    content
                                );
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
                                            println!("Utente {} è ora in stato: In Movimento", client.username);
                                        }
                                    };


                                    /*
                                     * Confermiamo al client
                                     * che il logout è riuscito.
                                     */
                                    let response =
                                        Message::LogoutResponse {
                                            success: true,
                                            message:
                                                "Logout effettuato correttamente"
                                                    .to_string(),
                                        };


                                    let response_json =
                                        serde_json::to_string(
                                            &response
                                        )? + "\n";


                                    write_half
                                        .write_all(
                                            response_json.as_bytes()
                                        )
                                        .await?;


                                    if let Some(username) =
                                        username
                                    {

                                        println!(
                                            "Utente {} disconnesso",
                                            username
                                        );
                                    }


                                    /*
                                     * La sessione è già stata
                                     * eliminata dalla HashMap.
                                     *
                                     * Mettiamo None così il cleanup
                                     * finale non tenta di eliminarla
                                     * una seconda volta.
                                     */
                                    current_user_id =
                                        None;


                                    /*
                                     * Terminiamo il loop che gestisce
                                     * questa specifica connessione TCP.
                                     */
                                    break;

                                } else {

                                    /*
                                     * Lo user_id ricevuto non coincide
                                     * con quello della sessione.
                                     */
                                    let response =
                                        Message::LogoutResponse {
                                            success: false,
                                            message:
                                                "Sessione non valida"
                                                    .to_string(),
                                        };


                                    let response_json =
                                        serde_json::to_string(
                                            &response
                                        )? + "\n";


                                    write_half
                                        .write_all(
                                            response_json.as_bytes()
                                        )
                                        .await?;
                                }
                            }


                            // =========================================
                            // ALTRI MESSAGGI
                            // =========================================

                            _ => {}
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

    // ========================================================
    // CLEANUP DISCONNESSIONE IMPREVISTA
    // ========================================================
    //
    // Questo blocco serve quando il client NON usa /logout,
    // ad esempio:
    //
    // - chiude il programma
    // - perde la connessione
    // - crasha
    //
    // Se invece ha fatto /logout,
    // current_user_id è già None.
    // in questo modo salviamo anche lo storico

    if let Some(user_id) = current_user_id {
        let mut w_state = state.write().await;
        if let Some(client) = w_state.clients.get_mut(&user_id) {
            client.state = UserState::Disconnected;
            let now = Utc::now();
            client.state_history.push((UserState::Disconnected, now));
            let _ = db::insert_state(&w_state.db_pool, &user_id, "Sconnesso", now);
        }
    }

    Ok(())
}

// ============================================================
// MONITORAGGIO STATO UTENTI
// ============================================================

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
                    /*
                     * Se non ci sono aggiornamenti
                     * di movimento per 3 minuti,
                     * passa allo stato Fermo.
                     */
                    if now.signed_duration_since(last_time).num_minutes() >= 3 {
                        client.state = UserState::Fermo;

                        client.state_history.push((UserState::Fermo, now));
                        let _ = db::insert_state(&pool, user_id, "Fermo", now);
                        println!("Utente {} passato a stato Fermo per inattività", client.username);
                    }
                }
            }
        }
    }
}

// ============================================================
// CPU LOGGER
// ============================================================

async fn cpu_logger_task() {
    let mut sys = System::new();

    let pid = get_current_pid().unwrap();

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(120));

    // Primo refresh
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
            let log_line = format!(
                "[{}] Server Process CPU Usage: {:.2}%\n",
                Utc::now(),
                cpu_usage
            );

            let _ = file.write_all(log_line.as_bytes());
        }
    }
}
