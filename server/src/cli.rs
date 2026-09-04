use chrono::{Datelike, TimeZone, Utc};

use shared::{Message, UserState};

use tokio::io::{AsyncBufReadExt, BufReader};

use crate::analysis;
use crate::db;
use crate::state::SharedState;


// ============================================================
// LOOP COMANDI SERVER
// ============================================================

pub async fn command_loop(state: SharedState) {
    let stdin = tokio::io::stdin();
    let mut reader = BufReader::new(stdin);
    let mut input = String::new();

    loop {
        input.clear();

        let bytes = match reader.read_line(&mut input).await {
            Ok(bytes) => bytes,
            Err(e) => {
                eprintln!("Errore lettura comando server: {}", e);
                continue;
            }
        };

        if bytes == 0 {
            break;
        }

        let text = input.trim();

        if text.is_empty() {
            continue;
        }

        if text.starts_with("/msg ") {
            handle_private_message(text, &state).await;
        } else if text.starts_with("/stats") {
            handle_stats(text, &state).await;
        } else if text.starts_with("/b ") {
            handle_broadcast(text, &state).await;
        } else if text.starts_with("/chat ") {
            handle_chat(text, &state).await;
        } else {
            print_help();
        }
    }
}


// ============================================================
// MESSAGGIO PRIVATO
// ============================================================

async fn handle_private_message(text: &str, state: &SharedState) {
    let parts: Vec<&str> = text.splitn(3, ' ').collect();

    if parts.len() != 3 {
        println!("Uso corretto: /msg <utente> <messaggio>");
        return;
    }

    let target_name = parts[1];
    let msg_content = parts[2];

    let r_state = state.read().await;

    // FIXME: cambia nome del campo target_user_id
    let direct_msg = Message::ServerToClientDirect {
        target_user_id: "Server".to_string(),
        content: msg_content.to_string(),
    };

    let mut found = false;

    for (uid, client) in r_state.clients.iter() {
        if client.username == target_name {
            let _ = client.sender.send(direct_msg.clone()).await;

            let _ = db::insert_chat(
                &r_state.db_pool,
                "Server",
                Some(uid),
                msg_content,
                Utc::now(),
            );

            found = true;
        }
    }

    if found {
        println!("Messaggio privato inviato a {}", target_name);
    } else {
        println!(
            "Utente {} non trovato o non connesso",
            target_name
        );
    }
}


// ============================================================
// BROADCAST
// ============================================================

async fn handle_broadcast(text: &str, state: &SharedState) {
    let msg_content = text
        .strip_prefix("/b ")
        .unwrap()
        .trim();

    let broadcast_msg = Message::ServerToClientBroadcast {
        content: msg_content.to_string(),
    };

    let r_state = state.read().await;

    for (_, client) in r_state.clients.iter() {
        let _ = client
            .sender
            .send(broadcast_msg.clone())
            .await;
    }

    println!("Messaggio broadcast inviato a tutti");
}


// ============================================================
// STATISTICHE
// ============================================================

async fn handle_stats(text: &str, state: &SharedState) {
    let parts: Vec<&str> = text.split_whitespace().collect();

    if parts.len() != 3 {
        println!(
            "Uso corretto: /stats <utente> <giorno|settimana|mese|all>"
        );
        return;
    }

    let target_name = parts[1];
    let interval = parts[2];

    if interval != "giorno"
        && interval != "settimana"
        && interval != "mese"
        && interval != "all"
    {
        println!(
            "Intervallo '{}' non valido. Usa: giorno, settimana, mese, all",
            interval
        );
        return;
    }

    let end_time = Utc::now();

    let start_time = match interval {
        "giorno" => {
            Utc.with_ymd_and_hms(
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
            let days_from_monday =
                end_time.weekday().num_days_from_monday();

            let monday =
                end_time - chrono::Duration::days(days_from_monday as i64);

            Utc.with_ymd_and_hms(
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
            Utc.with_ymd_and_hms(
                end_time.year(),
                end_time.month(),
                1,
                0,
                0,
                0,
            )
                .unwrap()
        }

        _ => chrono::DateTime::<Utc>::MIN_UTC,
    };

    let r_state = state.read().await;

    match db::get_user_by_name(
        &r_state.db_pool,
        target_name,
    ) {
        Ok(Some((uid, _))) => {
            match db::get_user_history(
                &r_state.db_pool,
                &uid,
                start_time,
                end_time,
            ) {
                Ok((states, distances)) => {
                    let result = analysis::analyze_movement(
                        &states,
                        &distances,
                        start_time,
                        end_time,
                    );

                    let mut state_str = "Disconnesso";

                    for (_, client) in r_state.clients.iter() {
                        if client.username == target_name {
                            state_str = match client.state {
                                UserState::Fermo => "Fermo",

                                UserState::InMovimento => {
                                    "In Movimento"
                                }

                                UserState::Disconnesso => {
                                    "Disconnesso"
                                }
                            };

                            break;
                        }
                    }

                    println!(
                        "=== STATISTICHE per {} ({}) ===\n\
                         Stato Attuale: {}\n\
                         Distanza: {:.2} km\n\
                         Velocità Media: {:.2} km/h\n\
                         Tempo in Movimento: {} sec\n\
                         Tempo Pause: {} sec\n\
                         ===============================",
                        target_name,
                        interval,
                        state_str,
                        result.total_distance_km,
                        result.average_speed_kmh,
                        result.moving_time_secs,
                        result.pause_time_secs
                    );
                }

                Err(e) => {
                    println!(
                        "Errore nel recupero storico: {}",
                        e
                    );
                }
            }
        }

        Ok(None) => {
            println!(
                "Utente {} non trovato nel database.",
                target_name
            );
        }

        Err(e) => {
            println!("Errore DB: {}", e);
        }
    }
}


// ============================================================
// STORICO CHAT
// ============================================================

async fn handle_chat(text: &str, state: &SharedState) {
    let parts: Vec<&str> = text.split_whitespace().collect();

    if parts.len() != 2 {
        println!("Uso corretto: /chat <utente>");
        return;
    }

    let target_name = parts[1];

    let r_state = state.read().await;

    match db::get_user_by_name(
        &r_state.db_pool,
        target_name,
    ) {
        Ok(Some((uid, _))) => {
            match db::get_chat_history(
                &r_state.db_pool,
                &uid,
            ) {
                Ok(chats) => {
                    println!(
                        "=== STORICO CHAT con {} ===",
                        target_name
                    );

                    if chats.is_empty() {
                        println!("(Nessun messaggio)");
                    } else {
                        for (sender, content, ts) in chats {
                            let display_sender =
                                if sender == "Server" {
                                    "Server"
                                } else {
                                    target_name
                                };

                            println!(
                                "[{}] {}: {}",
                                ts.format("%Y-%m-%d %H:%M:%S"),
                                display_sender,
                                content
                            );
                        }
                    }

                    println!(
                        "==============================="
                    );
                }

                Err(e) => {
                    println!(
                        "Errore nel recupero chat: {}",
                        e
                    );
                }
            }
        }

        Ok(None) => {
            println!(
                "Utente {} non trovato nel database.",
                target_name
            );
        }

        Err(e) => {
            println!("Errore DB: {}", e);
        }
    }
}


// ============================================================
// HELP
// ============================================================

fn print_help() {
    println!("--- Menu Comandi Server ---");
    println!(
        "/msg <utente> <testo>  : Invia un messaggio privato a un utente"
    );
    println!(
        "/b <testo>             : Invia un messaggio broadcast a tutti"
    );
    println!(
        "/stats <utente> <int.> : Mostra le statistiche (all, giorno, settimana, mese)"
    );
    println!(
        "/chat <utente>         : Mostra lo storico dei messaggi con un utente"
    );
    println!("---------------------------");
}