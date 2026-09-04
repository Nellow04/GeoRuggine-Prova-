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

    let direct_msg = Message::ServerToClientDirect {
        target_user_id: "Server".to_string(),
        content: msg_content.to_string(),
    };

    // 1. Acquisiamo brevemente il ReadLock solo per estrarre il channel sender e l'ID utente.
    // Il lock viene rilasciato IMMEDIATAMENTE all'uscita dal blocco.
    let target_info = {
        let clients = state.clients.read().await;
        clients
            .iter()
            .find(|(_, client)| client.username == target_name)
            .map(|(uid, client)| (uid.clone(), client.sender.clone()))
    };

    // 2. Eseguiamo gli .await di rete e le query DB completamente all'esterno del lock!
    if let Some((target_uid, sender)) = target_info {
        let _ = sender.send(direct_msg).await;

        let _ = db::insert_chat(
            &state.db_pool,
            "Server",
            Some(&target_uid),
            msg_content,
            Utc::now(),
        );

        println!("Messaggio privato inviato a {}", target_name);
    } else {
        println!("Utente {} non trovato tra i client connessi.", target_name);
    }
}

// ============================================================
// BROADCAST
// ============================================================

async fn handle_broadcast(text: &str, state: &SharedState) {
    let msg_content = text.strip_prefix("/b ").unwrap().trim();

    let broadcast_msg = Message::ServerToClientBroadcast {
        content: msg_content.to_string(),
    };

    // 1. Cloniamo i soli sender dei client all'interno di uno scope ristretto.
    // Il ReadLock su `clients` viene trattenuto per microsecondi e rilasciato subito.
    let senders: Vec<_> = {
        let clients = state.clients.read().await;
        clients.values().map(|c| c.sender.clone()).collect()
    };

    // 2. Inviamo a tutti i client senza trattenere alcun lock durante gli `.await`.
    // In questo modo, se un client ha il buffer pieno o è lento, non blocca l'intero server!
    for sender in senders {
        let _ = sender.send(broadcast_msg.clone()).await;
    }

    println!("Messaggio broadcast inviato a tutti");
}

// ============================================================
// STATISTICHE
// ============================================================

pub fn calculate_interval_bounds(
    interval: &str,
    end_time: chrono::DateTime<Utc>,
) -> (chrono::DateTime<Utc>, chrono::DateTime<Utc>) {
    let start_time = match interval {
        // Giorno corrente: da oggi alle ore 00:00:00 UTC a ora
        "giorno" => Utc
            .with_ymd_and_hms(end_time.year(), end_time.month(), end_time.day(), 0, 0, 0)
            .unwrap(),

        // Settimana corrente: da lunedì della settimana in corso alle ore 00:00:00 UTC a ora
        "settimana" => {
            let days_from_monday = end_time.weekday().num_days_from_monday();
            let start_of_today = Utc
                .with_ymd_and_hms(end_time.year(), end_time.month(), end_time.day(), 0, 0, 0)
                .unwrap();
            start_of_today - chrono::Duration::days(days_from_monday as i64)
        }

        // Mese corrente: dal giorno 1 del mese alle ore 00:00:00 UTC a ora
        "mese" => Utc
            .with_ymd_and_hms(end_time.year(), end_time.month(), 1, 0, 0, 0)
            .unwrap(),

        // Tutto lo storico: dall'inizio dei tempi a ora
        _ => chrono::DateTime::<Utc>::MIN_UTC,
    };

    (start_time, end_time)
}

async fn handle_stats(text: &str, state: &SharedState) {
    let parts: Vec<&str> = text.split_whitespace().collect();

    if parts.len() != 3 {
        println!("Uso corretto: /stats <utente> <giorno|settimana|mese|all>");
        return;
    }

    let target_name = parts[1];
    let interval = parts[2];

    if interval != "giorno" && interval != "settimana" && interval != "mese" && interval != "all" {
        println!(
            "Intervallo '{}' non valido. Usa: giorno, settimana, mese, all",
            interval
        );
        return;
    }

    let (start_time, end_time) = calculate_interval_bounds(interval, Utc::now());

    // 1. Query SQLite e calcoli analitici eseguiti direttamente su db_pool SENZA alcun lock su `clients`!
    match db::get_user_by_name(&state.db_pool, target_name) {
        Ok(Some((uid, _))) => {
            match db::get_user_history(&state.db_pool, &uid, start_time, end_time) {
                Ok((states, distances)) => {
                    let result =
                        analysis::analyze_movement(&states, &distances, start_time, end_time);

                    // 2. Breve lettura dello stato in tempo reale dalla mappa dei client
                    let state_str = {
                        let clients = state.clients.read().await;
                        clients
                            .values()
                            .find(|c| c.username == target_name)
                            .map(|client| match client.state {
                                UserState::Fermo => "Fermo",
                                UserState::InMovimento => "In Movimento",
                                UserState::Disconnesso => "Disconnesso",
                            })
                            .unwrap_or("Disconnesso")
                    };

                    println!("=== STATISTICHE: {} (Intervallo: {}) ===", target_name, interval);
                    let start_str = if start_time == chrono::DateTime::<Utc>::MIN_UTC {
                        "Inizio storico".to_string()
                    } else {
                        start_time.format("%Y-%m-%d %H:%M:%S UTC").to_string()
                    };
                    println!(
                        "Periodo: da {} a {}",
                        start_str,
                        end_time.format("%Y-%m-%d %H:%M:%S UTC")
                    );
                    println!("Stato Attuale: {}", state_str);
                    println!("Distanza Totale: {:.2} km", result.total_distance_km);
                    println!("Velocita Media: {:.2} km/h", result.average_speed_kmh);

                    let moving_mins = result.moving_time_secs / 60;
                    let pause_mins = result.pause_time_secs / 60;
                    println!(
                        "Tempo in Movimento: {}h {}m ({} sec)",
                        moving_mins / 60,
                        moving_mins % 60,
                        result.moving_time_secs
                    );
                    println!(
                        "Tempo in Pausa: {}h {}m ({} sec)",
                        pause_mins / 60,
                        pause_mins % 60,
                        result.pause_time_secs
                    );
                    println!("==================================================");
                }
                Err(e) => {
                    println!("Errore nel recupero dello storico: {}", e);
                }
            }
        }
        Ok(None) => {
            println!("Utente {} non trovato.", target_name);
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

    // ZERO LOCK SU `clients`!
    // Le query al database accedono direttamente al db_pool che è già thread-safe.
    match db::get_user_by_name(&state.db_pool, target_name) {
        Ok(Some((uid, _))) => {
            match db::get_chat_history(&state.db_pool, &uid) {
                Ok(chats) => {
                    println!("=== STORICO CHAT con {} ===", target_name);

                    if chats.is_empty() {
                        println!("(Nessun messaggio)");
                    } else {
                        for (sender, content, ts) in chats {
                            let display_sender = if sender == "Server" {
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

                    println!("===============================");
                }
                Err(e) => {
                    println!("Errore nel recupero chat: {}", e);
                }
            }
        }
        Ok(None) => {
            println!("Utente {} non trovato nel database.", target_name);
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
    println!("  /msg <utente> <testo>  : Invia un messaggio privato a un utente");
    println!("  /b <testo>             : Invia un messaggio broadcast a tutti");
    println!("  /stats <utente> <int.> : Mostra le statistiche (all, giorno, settimana, mese)");
    println!("  /chat <utente>         : Mostra lo storico dei messaggi con un utente");
    println!("---------------------------");
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{TimeZone, Utc};

    #[test]
    fn test_calculate_interval_bounds_giorno() {
        // Venerdì 4 Settembre 2026 alle 19:15:30 UTC
        let now = Utc.with_ymd_and_hms(2026, 9, 4, 19, 15, 30).unwrap();
        let (start, end) = calculate_interval_bounds("giorno", now);

        assert_eq!(start, Utc.with_ymd_and_hms(2026, 9, 4, 0, 0, 0).unwrap());
        assert_eq!(end, now);
    }

    #[test]
    fn test_calculate_interval_bounds_settimana() {
        // Venerdì 4 Settembre 2026 alle 19:15:30 UTC -> Lunedì era il 31 Agosto 2026
        let now = Utc.with_ymd_and_hms(2026, 9, 4, 19, 15, 30).unwrap();
        let (start, end) = calculate_interval_bounds("settimana", now);

        assert_eq!(start, Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap());
        assert_eq!(end, now);

        // Domenica 6 Settembre 2026 -> Lunedì deve essere sempre il 31 Agosto 2026
        let sunday = Utc.with_ymd_and_hms(2026, 9, 6, 23, 0, 0).unwrap();
        let (start_sun, _) = calculate_interval_bounds("settimana", sunday);
        assert_eq!(start_sun, Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap());

        // Lunedì 31 Agosto 2026 alle 10:00 -> Lunedì 31 Agosto 00:00:00
        let monday = Utc.with_ymd_and_hms(2026, 8, 31, 10, 0, 0).unwrap();
        let (start_mon, _) = calculate_interval_bounds("settimana", monday);
        assert_eq!(start_mon, Utc.with_ymd_and_hms(2026, 8, 31, 0, 0, 0).unwrap());
    }

    #[test]
    fn test_calculate_interval_bounds_mese() {
        // Venerdì 4 Settembre 2026 -> 1 Settembre 2026 00:00:00
        let now = Utc.with_ymd_and_hms(2026, 9, 4, 19, 15, 30).unwrap();
        let (start, end) = calculate_interval_bounds("mese", now);

        assert_eq!(start, Utc.with_ymd_and_hms(2026, 9, 1, 0, 0, 0).unwrap());
        assert_eq!(end, now);
    }

    #[test]
    fn test_calculate_interval_bounds_all() {
        let now = Utc.with_ymd_and_hms(2026, 9, 4, 19, 15, 30).unwrap();
        let (start, end) = calculate_interval_bounds("all", now);

        assert_eq!(start, chrono::DateTime::<Utc>::MIN_UTC);
        assert_eq!(end, now);
    }
}