use chrono::Utc;

use shared::UserState;

use std::fs::OpenOptions;
use std::io::Write;

use sysinfo::{
    get_current_pid,
    ProcessExt,
    System,
    SystemExt,
};

use crate::db;
use crate::state::SharedState;


// ============================================================
// MONITOR STATO UTENTI
// ============================================================

pub async fn state_monitor_task(state: SharedState) {
    let mut interval =
        tokio::time::interval(
            tokio::time::Duration::from_secs(30)
        );

    loop {
        interval.tick().await;

        let now = Utc::now();

        let mut w_state = state.write().await;

        let pool = w_state.db_pool.clone();

        for (user_id, client) in w_state.clients.iter_mut() {
            if client.state == UserState::InMovimento {
                if let Some(last_time) = client.last_move_time {

                    // Se non ci sono aggiornamenti
                    // di movimento per 3 minuti
                    if now
                        .signed_duration_since(last_time)
                        .num_minutes()
                        >= 3
                    {
                        client.state = UserState::Fermo;

                        client.state_history.push((
                            UserState::Fermo,
                            now,
                        ));

                        let _ = db::insert_state(
                            &pool,
                            user_id,
                            "Fermo",
                            now,
                        );
                    }
                }
            }
        }
    }
}


// ============================================================
// LOGGER CPU
// ============================================================

pub async fn cpu_logger_task() {
    let mut sys = System::new();

    let pid = get_current_pid().unwrap();

    let mut interval =
        tokio::time::interval(
            tokio::time::Duration::from_secs(120)
        );

    // Primo refresh necessario per inizializzare
    // il calcolo della CPU
    sys.refresh_process(pid);

    loop {
        interval.tick().await;

        sys.refresh_process(pid);

        let cpu_usage =
            if let Some(process) = sys.process(pid) {
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

            let _ =
                file.write_all(log_line.as_bytes());
        }
    }
}