mod auth;

use chrono::Utc;
use rand::Rng;
use shared::{Coordinates, Message};
use std::time::Duration;

use tokio::io::{
    AsyncBufReadExt,
    AsyncWriteExt,
    BufReader,
};


#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    // ============================================================
    // 1. AUTENTICAZIONE
    // ============================================================
    //
    // auth::authenticate() gestisce:
    // - scelta Login / Registrazione
    // - controllo input
    // - username e password
    // - connessione TCP
    // - registrazione
    // - login automatico dopo la registrazione
    //
    // Se il login riesce restituisce:
    // - user_id
    // - metà TCP per scrivere
    // - metà TCP per leggere, contenuta nel BufReader

    let (user_id, mut write_half, mut reader) =
        auth::authenticate().await?;


    // ============================================================
    // 2. SIMULAZIONE GPS
    // ============================================================

    // Copia dello user_id da spostare dentro il task GPS
    let user_id_clone = user_id.clone();

    /*
     * Canale interno al client.
     *
     * Il task GPS genera PositionUpdate e li invia tramite tx_gps.
     *
     * Il main riceve questi messaggi tramite rx_gps
     * e successivamente li invia al server tramite TCP.
     */
    let (tx_gps, mut rx_gps) =
        tokio::sync::mpsc::channel::<Message>(10);


    tokio::spawn(async move {

        // Coordinate iniziali
        let mut lat = 45.0;
        let mut lon = 7.0;

        // Numero di cicli per cui il veicolo deve restare fermo
        let mut pause_counter = 0;


        loop {

            // ----------------------------------------------------
            // VEICOLO IN PAUSA
            // ----------------------------------------------------

            if pause_counter > 0 {

                pause_counter -= 1;

                // Non modifichiamo latitudine e longitudine:
                // la posizione rimane invariata.


                // ----------------------------------------------------
                // VEICOLO NON IN PAUSA
                // ----------------------------------------------------

            } else {

                /*
                 * Generiamo un numero casuale tra 0 e 1.
                 *
                 * Se è < 0.15:
                 *      il veicolo inizia una sosta.
                 *
                 * Altrimenti:
                 *      il veicolo cambia posizione.
                 */
                let rng: f64 =
                    rand::thread_rng().gen();


                if rng < 0.15 {

                    /*
                     * Impostiamo una pausa.
                     *
                     * Ogni ciclo dura 30 secondi.
                     * 8 cicli corrispondono a circa 4 minuti.
                     */
                    pause_counter = 8;

                } else {

                    // Random walk
                    // FIXME: valutare se ridurre il raggio

                    lat += rand::thread_rng()
                        .gen_range(-0.01..0.01);

                    lon += rand::thread_rng()
                        .gen_range(-0.01..0.01);
                }
            }


            // Creiamo la nuova posizione
            let coords = Coordinates {
                latitude: lat,
                longitude: lon,
            };


            // Creiamo il messaggio da inviare al server
            let msg = Message::PositionUpdate {
                user_id: user_id_clone.clone(),
                coords,
                timestamp: Utc::now(),
            };


            /*
             * Il GPS non scrive direttamente sul TCP.
             *
             * Invia invece il messaggio al main tramite
             * il channel tx_gps -> rx_gps.
             */
            if tx_gps.send(msg).await.is_err() {
                break;
            }


            // Requisito della traccia:
            // una posizione ogni 30 secondi
            tokio::time::sleep(
                Duration::from_secs(30)
            )
                .await;
        }
    });


    // ============================================================
    // 3. INPUT CONSOLE PER LA CHAT
    // ============================================================

    // Copia dello user_id da spostare nel task della console
    let user_id_cli = user_id.clone();


    /*
     * Secondo channel interno al client.
     *
     * tx_cli:
     *      usato dal task che legge la tastiera
     *
     * rx_cli:
     *      usato dal main per ricevere i messaggi
     *      e mandarli al server
     */
    let (tx_cli, mut rx_cli) =
        tokio::sync::mpsc::channel::<Message>(10);


    tokio::spawn(async move {

        let stdin = tokio::io::stdin();

        let mut reader =
            BufReader::new(stdin);

        let mut input = String::new();


        loop {

            input.clear();


            if let Ok(bytes) =
                reader.read_line(&mut input).await
            {

                // stdin chiuso
                if bytes == 0 {
                    break;
                }


                let text =
                    input.trim().to_string();


                // Ignoriamo righe vuote
                if text.is_empty() {
                    continue;
                }


                /*
                 * Il client può mandare messaggi al server
                 * utilizzando:
                 *
                 * /msg testo
                 */
                if text.starts_with("/msg ") {

                    let msg_content =
                        text
                            .strip_prefix("/msg ")
                            .unwrap()
                            .trim();


                    let msg =
                        Message::ClientToServerText {
                            user_id:
                            user_id_cli.clone(),

                            content:
                            msg_content.to_string(),
                        };


                    // Mandiamo il messaggio al main
                    if tx_cli.send(msg).await.is_err() {
                        break;
                    }

                } else {

                    println!(
                        "Sistema: usa il comando \
                         /msg <testo> per inviare \
                         un messaggio al server"
                    );
                }
            }
        }
    });


    // ============================================================
    // 4. MAIN LOOP DEL CLIENT
    // ============================================================
    //
    // Il client deve gestire contemporaneamente:
    //
    // 1. messaggi ricevuti dal server
    // 2. aggiornamenti GPS
    // 3. messaggi scritti dall'utente
    //
    // tokio::select! aspetta che una qualsiasi
    // di queste operazioni sia pronta.

    let mut line = String::new();


    loop {

        line.clear();


        tokio::select! {


            // ====================================================
            // RICEZIONE DAL SERVER
            // ====================================================

            bytes_read = reader.read_line(&mut line) => {

                let bytes = bytes_read?;


                // Se leggiamo 0 byte,
                // il server ha chiuso la connessione
                if bytes == 0 {

                    println!(
                        "Disconnesso dal server."
                    );

                    break;
                }


                /*
                 * Trasformiamo il JSON ricevuto
                 * in un Message Rust.
                 */
                if let Ok(msg) =
                    serde_json::from_str::<Message>(&line)
                {

                    match msg {


                        // ----------------------------------------
                        // Broadcast
                        // ----------------------------------------

                        Message::ServerToClientBroadcast {
                            content
                        } => {

                            println!(
                                "[BROADCAST]: {}",
                                content
                            );
                        }


                        // ----------------------------------------
                        // Messaggio diretto
                        // ----------------------------------------

                        Message::ServerToClientDirect {
                            target_user_id: _,
                            content
                        } => {

                            println!(
                                "[MESSAGGIO]: {}",
                                content
                            );
                        }


                        // Altri tipi di messaggio
                        // non vengono gestiti qui
                        _ => {}
                    }
                }
            }


            // ====================================================
            // INVIO GPS AL SERVER
            // ====================================================

            Some(msg) = rx_gps.recv() => {

                /*
                 * Riceviamo un PositionUpdate
                 * dal task GPS.
                 */

                let json =
                    serde_json::to_string(&msg)?
                    + "\n";


                /*
                 * Qui avviene la vera comunicazione
                 * client -> server tramite TCP.
                 */
                write_half
                    .write_all(json.as_bytes())
                    .await?;
            }


            // ====================================================
            // INVIO CHAT AL SERVER
            // ====================================================

            Some(msg) = rx_cli.recv() => {

                /*
                 * Riceviamo il messaggio dal task
                 * che legge la tastiera.
                 */

                let json =
                    serde_json::to_string(&msg)?
                    + "\n";


                // Invio tramite TCP
                write_half
                    .write_all(json.as_bytes())
                    .await?;
            }
        }
    }


    Ok(())
}