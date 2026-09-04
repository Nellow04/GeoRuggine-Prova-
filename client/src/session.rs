use std::error::Error;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
use shared::Message;
use crate::auth::AuthResult;
use crate::{cli, gps};

/// Esegue l'orchestrazione di una sessione attiva dopo il login.
///
/// Questa funzione:
/// 1. Inizializza i canali interni MPSC per la comunicazione tra task.
/// 2. Avvia i task di supporto: `gps::spawn_gps_task` e `cli::spawn_cli_task`.
/// 3. Esegue il ciclo principale con `tokio::select!` per multiplexare:
///    - Ricezione di messaggi TCP dal server (Broadcast, Diretti, LogoutResponse).
///    - Invio dei rilevamenti GPS al server.
///    - Invio dei comandi da console (messaggi chat o richiesta di logout).
/// 4. Gestisce il graceful shutdown al logout (interruzione dei task e rilascio socket).
pub async fn run_session(auth_result: AuthResult) -> Result<(), Box<dyn Error>> {
    let (user_id, mut write_half, mut reader) = auth_result;

    // Canali interni MPSC con buffer limitato per disaccoppiare i task dalla rete
    let (tx_gps, mut rx_gps) = tokio::sync::mpsc::channel::<Message>(10);
    let (tx_cli, mut rx_cli) = tokio::sync::mpsc::channel::<Message>(10);

    // Avvio dei background task
    let gps_handle = gps::spawn_gps_task(user_id.clone(), tx_gps);
    let cli_handle = cli::spawn_cli_task(user_id.clone(), tx_cli);

    let mut line = String::new();

    // Flag per tracciare se è stata inoltrata una LogoutRequest e siamo in attesa di conferma
    let mut logout_requested = false;

    // ========================================================================
    // EVENT LOOP ASINCRONO DELLA SESSIONE (Multiplexing con tokio::select!)
    // ========================================================================
    loop {
        line.clear();

        tokio::select! {
            // ----------------------------------------------------------------
            // 1. RICEZIONE DAL SERVER (TCP Socket -> Client)
            // ----------------------------------------------------------------
            bytes_read = reader.read_line(&mut line) => {
                let bytes = bytes_read?;

                // Connessione chiusa dal server
                if bytes == 0 {
                    println!("Disconnesso dal server.");
                    break;
                }

                let msg = match serde_json::from_str::<Message>(&line) {
                    Ok(msg) => msg,
                    Err(e) => {
                        eprintln!("Messaggio non valido ricevuto dal server: {}", e);
                        continue;
                    }
                };

                match msg {
                    Message::ServerToClientBroadcast { content } => {
                        println!("[BROADCAST]: {}", content);
                    }
                    Message::ServerToClientDirect { content, .. } => {
                        println!("[MESSAGGIO]: {}", content);
                    }
                    Message::LogoutResponse { success, message } => {
                        println!("Server: {}", message);

                        if success {
                            // Il server ha confermato il logout: possiamo terminare la sessione
                            break;
                        } else {
                            logout_requested = false;
                            println!("Logout non riuscito.");
                        }
                    }
                    _ => {}
                }
            }

            // ----------------------------------------------------------------
            // 2. INVIO AGGIORNAMENTI GPS AL SERVER (GPS Task -> TCP Socket)
            // ----------------------------------------------------------------
            Some(msg) = rx_gps.recv(), if !logout_requested => {
                let json = serde_json::to_string(&msg)? + "\n";
                write_half.write_all(json.as_bytes()).await?;
            }

            // ----------------------------------------------------------------
            // 3. INVIO COMANDI CONSOLE AL SERVER (CLI Task -> TCP Socket)
            // ----------------------------------------------------------------
            Some(msg) = rx_cli.recv(), if !logout_requested => {
                let is_logout = matches!(&msg, Message::LogoutRequest { .. });
                let json = serde_json::to_string(&msg)? + "\n";

                write_half.write_all(json.as_bytes()).await?;

                if is_logout {
                    // Impostiamo il flag: non inviamo ulteriori dati finché il server non risponde
                    logout_requested = true;
                    println!("Logout in corso...");
                }
            }
        }
    }

    // ========================================================================
    // TEARDOWN E PULIZIA RISORSE DELLA SESSIONE
    // ========================================================================
    gps_handle.abort();
    cli_handle.abort();

    drop(write_half);
    drop(reader);

    println!("Sessione terminata.\n");

    Ok(())
}
