use chrono::Utc;
use shared::{Message, UserId, UserState};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::analysis;
use crate::auth::{hash_password, verify_password};
use crate::db;
use crate::state::{ClientData, SharedState};

// ============================================================
// GESTIONE CLIENT CONNESSO
// ============================================================

pub async fn handle_client(
    mut socket: TcpStream,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error>> {
    let (read_half, mut write_half) = socket.split();
    let mut reader = BufReader::new(read_half);
    let mut line = String::new();

    let mut current_user_id: Option<UserId> = None;

    // Canale per inoltrare al client i messaggi generati dal server (broadcast o diretti)
    let (tx, mut rx) = mpsc::channel::<Message>(32);

    loop {
        line.clear();

        tokio::select! {
            // =================================================
            // 1. MESSAGGIO RICEVUTO DAL CLIENT
            // =================================================
            bytes_read = reader.read_line(&mut line) => {
                let bytes_read = match bytes_read {
                    Ok(b) => b,
                    Err(e) => {
                        eprintln!("Errore lettura client: {}", e);
                        break;
                    }
                };

                // Socket chiuso dal client
                if bytes_read == 0 {
                    break;
                }

                let msg: Message = match serde_json::from_str(&line) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("Errore parsing JSON: {}", e);
                        continue;
                    }
                };

                match msg {
                    // =========================================
                    // REGISTRAZIONE
                    // =========================================
                    Message::RegisterRequest { username, password } => {
                        // ZERO LOCK SU `clients`!
                        // La registrazione interagisce esclusivamente con il DB e calcola l'hash.
                        // Entrambe le operazioni sono asincrone e non bloccano il worker thread Tokio.
                        let hashed_password = match hash_password(&password).await {
                            Ok(h) => h,
                            Err(e) => {
                                eprintln!("Errore hash password: {}", e);
                                continue;
                            }
                        };

                        let new_id = uuid::Uuid::new_v4().to_string();

                        match db::register_user(&state.db_pool, &new_id, &username, &hashed_password).await {
                            Ok(true) => {
                                let response = Message::RegisterResponse {
                                    success: true,
                                    message: "Registrazione completata".to_string(),
                                };
                                let response_json = serde_json::to_string(&response)? + "\n";

                                // Await di rete eseguito SENZA trattenere alcun lock!
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
                    }

                    // =========================================
                    // LOGIN
                    // =========================================
                    Message::LoginRequest { username, password } => {
                        // 1. Controllo duplicati con breve ReadLock su `clients`
                        let is_already_connected = {
                            let clients = state.clients.read().await;
                            clients.values().any(|c| {
                                c.username == username && c.state != UserState::Disconnesso
                            })
                        };

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

                        // 2. Query DB e verifica della password (CPU-bound Argon2) eseguite in background (spawn_blocking) SENZA lock!
                        match db::get_user_by_name(&state.db_pool, &username).await {
                            Ok(Some((db_id, db_hash))) => {
                                if verify_password(&password, &db_hash).await {
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

                                    // 3. Controllo atomico definitivo (Double-Checked Locking) e inserimento nel WriteLock
                                    let login_conflict = {
                                        let mut clients = state.clients.write().await;
                                        if clients.values().any(|c| {
                                             c.username == username && c.state != UserState::Disconnesso
                                        }) {
                                            true
                                        } else {
                                            clients.insert(db_id.clone(), client_data);
                                            false
                                        }
                                    }; // <-- LOCK RILASCIATO IMMEDIATAMENTE!

                                    if login_conflict {
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

                                    current_user_id = Some(db_id.clone());

                                    // 4. Scrittura stato iniziale nel DB in background (a lock rilasciato)
                                    let _ = db::insert_state(&state.db_pool, &db_id, "Fermo", now).await;

                                    // 5. Invio risposta al client via socket TCP (a lock rilasciato)
                                    let response = Message::LoginResponse {
                                        success: true,
                                        user_id: Some(db_id.clone()),
                                        message: format!("Benvenuto {}", username),
                                    };
                                    let response_json = serde_json::to_string(&response)? + "\n";

                                    if write_half.write_all(response_json.as_bytes()).await.is_err() {
                                        break;
                                    }

                                    println!("Utente {} autenticato.", username);
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
                    }

                    // =========================================
                    // LOGOUT
                    // =========================================
                    Message::LogoutRequest { user_id } => {
                        if current_user_id.as_ref() == Some(&user_id) {
                            // 1. Rimuoviamo il client dalla memoria con un breve WriteLock
                            let removed_username = {
                                let mut clients = state.clients.write().await;
                                clients.remove(&user_id).map(|c| c.username)
                            };

                            let username = removed_username.unwrap_or_else(|| user_id.clone());

                            // 2. Registriamo il logout nel DB e inviamo la risposta a lock rilasciato
                            let _ = db::insert_state(&state.db_pool, &user_id, "Disconnesso", Utc::now()).await;

                            let response = Message::LogoutResponse {
                                success: true,
                                message: "Logout effettuato con successo".to_string(),
                            };
                            let response_json = serde_json::to_string(&response)? + "\n";

                            let _ = write_half.write_all(response_json.as_bytes()).await;

                            println!("Utente {} ha effettuato il logout", username);
                            current_user_id = None;
                        } else {
                            let response = Message::LogoutResponse {
                                success: false,
                                message: "Utente non autorizzato al logout".to_string(),
                            };
                            let response_json = serde_json::to_string(&response)? + "\n";

                            let _ = write_half.write_all(response_json.as_bytes()).await;
                        }
                    }

                    // =========================================
                    // AGGIORNAMENTO POSIZIONE GPS
                    // =========================================
                    Message::PositionUpdate { user_id, coords, timestamp } => {
                        if Some(user_id.clone()) == current_user_id {
                            // 1. Aggiorniamo coordinate, calcoliamo distanza e verifichiamo cambi di stato
                            // all'interno di un breve WriteLock, estraendo i dati da persistere.
                            let db_actions = {
                                let mut clients = state.clients.write().await;
                                if let Some(client) = clients.get_mut(&user_id) {
                                    if let Some(last_pos) = &client.last_position {
                                        let dist = analysis::calculate_distance(last_pos, &coords);
                                        client.distance_history.push((dist, timestamp));

                                        let mut state_changed = false;
                                        if dist > 0.001 {
                                            if client.state != UserState::InMovimento {
                                                client.state = UserState::InMovimento;
                                                client.state_history.push((UserState::InMovimento, timestamp));
                                                state_changed = true;
                                            }
                                            client.last_move_time = Some(timestamp);
                                        }

                                        client.last_position = Some(coords.clone());
                                        Some((dist, state_changed))
                                    } else {
                                        // Prima posizione registrata
                                        client.last_move_time = Some(timestamp);
                                        client.last_position = Some(coords.clone());
                                        None
                                    }
                                } else {
                                    None
                                }
                            }; // <-- IL WRITE LOCK VIENE RILASCIATO SUBITO QUI!

                            // 2. Le query di persistenza su SQLite avvengono a lock rilasciato in background
                            if let Some((dist, state_changed)) = db_actions {
                                let _ = db::insert_distance(&state.db_pool, &user_id, dist, timestamp).await;
                                if state_changed {
                                    let _ = db::insert_state(&state.db_pool, &user_id, "In Movimento", timestamp).await;
                                }
                            }
                        }
                    }

                    // =========================================
                    // MESSAGGIO CLIENT -> SERVER
                    // =========================================
                    Message::ClientToServerText { user_id, content } => {
                        // 1. Breve ReadLock per ricavare lo username del mittente (NO WriteLock!)
                        let sender_name = {
                            let clients = state.clients.read().await;
                            clients
                                .get(&user_id)
                                .map(|c| c.username.clone())
                                .unwrap_or_else(|| user_id.clone())
                        };

                        // 2. Salvataggio su database a lock rilasciato in background
                        let _ = db::insert_chat(
                            &state.db_pool,
                            &user_id,
                            Some("Server"),
                            &content,
                            Utc::now(),
                        ).await;

                        println!("[Messaggio da {}]: {}", sender_name, content);
                    }

                    _ => {}
                }
            }

            // =================================================
            // 2. MESSAGGIO SERVER -> CLIENT (Inoltro da channel)
            // =================================================
            Some(out_msg) = rx.recv() => {
                let json = serde_json::to_string(&out_msg)? + "\n";
                if write_half.write_all(json.as_bytes()).await.is_err() {
                    break;
                }
            }
        }
    }

    // =========================================================
    // DISCONNESSIONE IMPREVISTA / FINE STREAM
    // =========================================================
    if let Some(user_id) = current_user_id {
        let username = {
            let mut clients = state.clients.write().await;
            if let Some(client) = clients.get_mut(&user_id) {
                client.state = UserState::Disconnesso;
                let now = Utc::now();
                client.state_history.push((UserState::Disconnesso, now));
                Some(client.username.clone())
            } else {
                None
            }
        };

        // Scrittura dello stato disconnesso nel DB a lock rilasciato in background
        let _ = db::insert_state(&state.db_pool, &user_id, "Disconnesso", Utc::now()).await;

        if let Some(name) = username {
            println!("Utente {} disconnesso.", name);
        }
    }

    Ok(())
}