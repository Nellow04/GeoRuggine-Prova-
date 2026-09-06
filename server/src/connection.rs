use chrono::Utc;
use shared::{Message, UserId, UserState};
use tokio::io::{AsyncBufReadExt, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::analysis;
use crate::auth::{hash_password, verify_password};
use crate::db;
use crate::state::{ClientData, SharedState};

// ============================================================
// GESTIONE CLIENT CONNESSO
// ============================================================

async fn send_to_client<W>(write_half: &mut W, message: &Message) -> bool
where
    W: AsyncWrite + Unpin,
{
    let response_json = match serde_json::to_string(message) {
        Ok(json) => json + "\n",
        Err(e) => {
            eprintln!("Errore serializzazione risposta client: {}", e);
            return false;
        }
    };

    if let Err(e) = write_half.write_all(response_json.as_bytes()).await {
        eprintln!("Errore invio risposta al client: {}", e);
        return false;
    }

    true
}

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
                        // Le operazioni bloccanti su SQLite e Argon2 vengono eseguite
                        // tramite spawn_blocking, senza occupare i worker async di Tokio.
                        let hashed_password = match hash_password(&password).await {
                            Ok(h) => h,
                            Err(e) => {
                                eprintln!("Errore hash password: {}", e);
                                let response = Message::RegisterResponse {
                                    success: false,
                                    message: "Errore interno del server".to_string(),
                                };
                                if !send_to_client(&mut write_half, &response).await {
                                    break;
                                }
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

                                // Await di rete eseguito SENZA trattenere alcun lock!
                                if !send_to_client(&mut write_half, &response).await {
                                    break;
                                }

                                println!("Nuovo utente registrato: {}", username);
                            }
                            Ok(false) => {
                                let response = Message::RegisterResponse {
                                    success: false,
                                    message: "Utente già esistente".to_string(),
                                };

                                if !send_to_client(&mut write_half, &response).await {
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Errore DB in registrazione: {}", e);
                                let response = Message::RegisterResponse {
                                    success: false,
                                    message: "Errore interno del server".to_string(),
                                };
                                if !send_to_client(&mut write_half, &response).await {
                                    break;
                                }
                            }
                        }
                    }

                    // =========================================
                    // LOGIN
                    // =========================================
                    Message::LoginRequest { username, password } => {

                        if current_user_id.is_some() {
                            let response = Message::LoginResponse {
                                success: false,
                                user_id: None,
                                message: "Sessione già autenticata".to_string(),
                            };

                            if !send_to_client(&mut write_half, &response).await {
                                break;
                            }

                            continue;
                        }
                        // 1. Controllo duplicati con breve ReadLock su `clients`
                        let is_already_connected = {
                            let clients = state.clients.read().await;
                            clients.values().any(|c| c.username == username)
                        };

                        if is_already_connected {
                            let response = Message::LoginResponse {
                                success: false,
                                user_id: None,
                                message: "Utente già connesso su un altro dispositivo".to_string(),
                            };

                            if !send_to_client(&mut write_half, &response).await {
                                break;
                            }
                            continue;
                        }

                        // Query DB e verifica della password vengono delegate al blocking pool
                        // tramite spawn_blocking, senza mantenere lock su clients.
                        match db::get_user_by_name(&state.db_pool, &username).await {
                            Ok(Some((db_id, db_hash))) => {
                                let password_is_valid = match verify_password(&password, &db_hash).await {
                                    Ok(is_valid) => is_valid,
                                    Err(e) => {
                                        eprintln!("Errore verifica password per {}: {}", username, e);
                                        let response = Message::LoginResponse {
                                            success: false,
                                            user_id: None,
                                            message: "Errore interno del server".to_string(),
                                        };
                                        if !send_to_client(&mut write_half, &response).await {
                                            break;
                                        }
                                        continue;
                                    }
                                };

                                if password_is_valid {
                                    let now = Utc::now();

                                    let client_data = ClientData {
                                        username: username.clone(),
                                        state: UserState::Fermo,
                                        last_position: None,
                                        last_move_time: None,
                                        sender: tx.clone(),
                                    };

                                    // 3. Controllo atomico definitivo (Double-Checked Locking) e inserimento nel WriteLock
                                    let login_conflict = {
                                        let mut clients = state.clients.write().await;
                                        if clients.values().any(|c| c.username == username) {
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

                                        if !send_to_client(&mut write_half, &response).await {
                                            break;
                                        }
                                        continue;
                                    }

                                    current_user_id = Some(db_id.clone());

                                    // 4. Scrittura dello stato iniziale nel DB a lock rilasciato
                                    if let Err(e) = db::insert_state(&state.db_pool, &db_id, "Fermo", now).await {
                                        eprintln!("Errore DB salvataggio stato iniziale per {}: {}", db_id, e);
                                    }

                                    // 5. Invio risposta al client via socket TCP (a lock rilasciato)
                                    let response = Message::LoginResponse {
                                        success: true,
                                        user_id: Some(db_id.clone()),
                                        message: format!("Benvenuto {}", username),
                                    };

                                    if !send_to_client(&mut write_half, &response).await {
                                        break;
                                    }

                                    println!("Utente {} autenticato.", username);
                                } else {
                                    let response = Message::LoginResponse {
                                        success: false,
                                        user_id: None,
                                        message: "Password errata".to_string(),
                                    };

                                    if !send_to_client(&mut write_half, &response).await {
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

                                if !send_to_client(&mut write_half, &response).await {
                                    break;
                                }
                            }
                            Err(e) => {
                                eprintln!("Errore DB in login: {}", e);
                                let response = Message::LoginResponse {
                                    success: false,
                                    user_id: None,
                                    message: "Errore interno del server".to_string(),
                                };
                                if !send_to_client(&mut write_half, &response).await {
                                    break;
                                }
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
                            if let Err(e) = db::insert_state(&state.db_pool, &user_id, "Disconnesso", Utc::now()).await {
                                eprintln!("Errore DB salvataggio logout per {}: {}", user_id, e);
                            }

                            current_user_id = None;

                            let response = Message::LogoutResponse {
                                success: true,
                                message: "Logout effettuato con successo".to_string(),
                            };

                            if !send_to_client(&mut write_half, &response).await {
                                break;
                            }

                            println!("Utente {} ha effettuato il logout", username);

                        } else {
                            let response = Message::LogoutResponse {
                                success: false,
                                message: "Utente non autorizzato al logout".to_string(),
                            };

                            if !send_to_client(&mut write_half, &response).await {
                                break;
                            }
                        }
                    }

                    // =========================================
                    // AGGIORNAMENTO POSIZIONE GPS
                    // =========================================
                    Message::PositionUpdate { user_id, coords, timestamp } => {
                        if Some(user_id.clone()) != current_user_id {
                            eprintln!("PositionUpdate non autorizzato per user_id: {}", user_id);
                            continue;
                        }

                        // 1. Aggiorniamo coordinate, calcoliamo distanza e verifichiamo cambi di stato
                        // all'interno di un breve WriteLock, estraendo i dati da persistere.
                        let db_actions = {
                            let mut clients = state.clients.write().await;
                            if let Some(client) = clients.get_mut(&user_id) {
                                if let Some(last_pos) = &client.last_position {
                                    let dist = analysis::calculate_distance(last_pos, &coords);

                                    let mut state_changed = false;
                                    if dist > 0.001 {
                                        if client.state != UserState::InMovimento {
                                            client.state = UserState::InMovimento;
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
                            if let Err(e) = db::insert_distance(&state.db_pool, &user_id, dist, timestamp).await {
                                eprintln!("Errore DB salvataggio distanza per {}: {}", user_id, e);
                            }
                            if state_changed {
                                if let Err(e) = db::insert_state(&state.db_pool, &user_id, "In Movimento", timestamp).await {
                                    eprintln!("Errore DB salvataggio stato movimento per {}: {}", user_id, e);
                                }
                            }
                        }
                    }

                    // =========================================
                    // MESSAGGIO CLIENT -> SERVER
                    // =========================================
                    Message::ClientToServerText { user_id, content } => {
                        // Verifichiamo che il client possa inviare messaggi
                        // solamente usando il proprio user_id autenticato.
                        if current_user_id.as_ref() != Some(&user_id) {
                            eprintln!(
                                "Tentativo di invio messaggio con user_id non autorizzato: {}",
                                user_id
                            );
                            continue;
                        }

                        // 1. Breve ReadLock per ricavare lo username del mittente
                        let sender_name = {
                            let clients = state.clients.read().await;

                            clients
                                .get(&user_id)
                                .map(|c| c.username.clone())
                                .unwrap_or_else(|| user_id.clone())
                        };

                        // Qui il ReadLock è già stato rilasciato.

                        // 2. Salvataggio del messaggio nel database
                        if let Err(e) = db::insert_chat(
                            &state.db_pool,
                            &user_id,
                            Some("Server"),
                            &content,
                            Utc::now(),
                        ).await {
                            eprintln!("Errore DB salvataggio messaggio da {}: {}", user_id, e);
                        }

                        // 3. Visualizzazione del messaggio sulla console del server
                        println!("[Messaggio da {}]: {}", sender_name, content);
                    }

                    _ => {}
                }
            }

            // =================================================
            // 2. MESSAGGIO SERVER -> CLIENT (Inoltro da channel)
            // =================================================
            Some(out_msg) = rx.recv() => {
                if !send_to_client(&mut write_half, &out_msg).await {
                    break;
                }
            }
        }
    }

    // =========================================================
    // DISCONNESSIONE IMPREVISTA / FINE STREAM
    // =========================================================
    if let Some(user_id) = current_user_id {
        // Rimuoviamo il client dalla memoria attiva per non lasciare zombie e canali morti
        let username = {
            let mut clients = state.clients.write().await;
            clients.remove(&user_id).map(|c| c.username)
        };

        // Scrittura dello stato disconnesso nel DB a lock rilasciato in background
        if let Err(e) = db::insert_state(&state.db_pool, &user_id, "Disconnesso", Utc::now()).await
        {
            eprintln!(
                "Errore DB salvataggio disconnessione per {}: {}",
                user_id, e
            );
        }

        if let Some(name) = username {
            println!("Utente {} disconnesso.", name);
        }
    }

    Ok(())
}
