use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use shared::Message;

/// Avvia in background il task asincrono incaricato di leggere i comandi utente da terminale.
///
/// I comandi supportati sono:
/// - `/msg <testo>`: invia un messaggio di testo al server.
/// - `/logout`: richiede la chiusura della sessione corrente.
///
/// I messaggi validati vengono convertiti in varianti dell'enum `Message` e spediti al
/// canale `tx_cli` per essere inviati sulla connessione di rete dal task di sessione.
pub fn spawn_cli_task(user_id: String, tx_cli: Sender<Message>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let stdin = tokio::io::stdin();
        let mut reader = BufReader::new(stdin);
        let mut input = String::new();

        loop {
            input.clear();

            let bytes = match reader.read_line(&mut input).await {
                Ok(bytes) => bytes,
                Err(e) => {
                    eprintln!("Errore lettura input console: {}", e);
                    break;
                }
            };

            // Se bytes == 0, lo stream di stdin è stato chiuso (EOF)
            if bytes == 0 {
                break;
            }

            let text = input.trim().to_string();

            // Ignora righe vuote o spazi
            if text.is_empty() {
                continue;
            }

            // Gestione comando /logout
            if text == "/logout" {
                let msg = Message::LogoutRequest {
                    user_id: user_id.clone(),
                };

                // Inviamo al loop principale per l'inoltro TCP
                let _ = tx_cli.send(msg).await;

                // Terminata la richiesta di logout, la console per questa sessione si ferma
                break;
            }
            // Gestione comando /msg <testo>
            else if text.starts_with("/msg ") {
                let msg_content = text.strip_prefix("/msg ").unwrap().trim();

                if msg_content.is_empty() {
                    println!("Sistema: il messaggio non può essere vuoto.");
                    continue;
                }

                let msg = Message::ClientToServerText {
                    user_id: user_id.clone(),
                    content: msg_content.to_string(),
                };

                if tx_cli.send(msg).await.is_err() {
                    break;
                }
            }
            // Comando non riconosciuto
            else {
                println!("Sistema: comandi disponibili:");
                println!("  /msg <testo>  - Invia un messaggio al server");
                println!("  /logout       - Termina la sessione corrente");
            }
        }
    })
}
