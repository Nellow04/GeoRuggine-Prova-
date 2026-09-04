use chrono::Utc;
use shared::{UserId, UserState};

use std::fs::OpenOptions;
use std::io::Write;

use sysinfo::{get_current_pid, ProcessExt, System, SystemExt};

use crate::db;
use crate::state::SharedState;

// ============================================================
// MONITOR STATO UTENTI
// ============================================================

/// Task periodico che controlla ogni 30 secondi se i client in movimento sono fermi da più di 3 minuti.
pub async fn state_monitor_task(state: SharedState) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

    loop {
        interval.tick().await;

        let now = Utc::now();

        // 1. Acquisiamo il WriteLock su `clients` solo per il tempo strettamente necessario
        // ad aggiornare la memoria e identificare chi è diventato "Fermo".
        let users_to_persist: Vec<UserId> = {
            let mut clients = state.clients.write().await;
            let mut to_update = Vec::new();

            for (user_id, client) in clients.iter_mut() {
                if client.state == UserState::InMovimento {
                    if let Some(last_time) = client.last_move_time {
                        if now.signed_duration_since(last_time).num_minutes() >= 3 {
                            client.state = UserState::Fermo;
                            client.state_history.push((UserState::Fermo, now));
                            to_update.push(user_id.clone());
                        }
                    }
                }
            }

            to_update
        }; // <-- IL WRITE LOCK SU `clients` VIENE RILASCIATO IMMEDIATAMENTE QUI!

        // 2. Le scritture su disco nel database SQLite avvengono SENZA trattenere il lock dei client,
        // garantendo che nessun'altra connessione o comando rimanga bloccato durante le operazioni I/O.
        for user_id in users_to_persist {
            let _ = db::insert_state(&state.db_pool, &user_id, "Fermo", now);
        }
    }
}

// ============================================================
// LOGGER CPU
// ============================================================

/// Task periodico che registra ogni 2 minuti l'utilizzo della CPU del processo server in un file di log.
pub async fn cpu_logger_task() {
    let mut sys = System::new();
    let pid = get_current_pid().unwrap();

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(120));

    // Primo refresh necessario per inizializzare il calcolo della CPU
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