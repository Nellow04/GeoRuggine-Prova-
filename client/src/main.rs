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

    /*
     * LOOP DELLE SESSIONI
     *
     * Ogni giro corrisponde a:
     *
     * login -> sessione -> logout
     *
     * Dopo il logout si torna automaticamente
     * all'inizio e viene mostrato di nuovo
     * il menu Login / Registrazione.
     */
    loop {

        // ============================================================
        // 1. AUTENTICAZIONE
        // ============================================================

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
         * Il task GPS genera PositionUpdate
         * e li manda al main tramite tx_gps.
         */
        let (tx_gps, mut rx_gps) =
            tokio::sync::mpsc::channel::<Message>(10);


        /*
         * Conserviamo il JoinHandle.
         *
         * In questo modo, al logout,
         * possiamo fermare il task GPS.
         */
        let gps_handle = tokio::spawn(async move {

            // Coordinate iniziali
            let mut lat = 45.0;
            let mut lon = 7.0;

            // Numero di cicli per cui il veicolo resta fermo
            let mut pause_counter = 0;


            loop {

                // ----------------------------------------------------
                // VEICOLO IN PAUSA
                // ----------------------------------------------------

                if pause_counter > 0 {

                    pause_counter -= 1;

                    /*
                     * Non modifichiamo latitudine e longitudine.
                     * La posizione rimane invariata.
                     */

                } else {

                    // ------------------------------------------------
                    // VEICOLO NON IN PAUSA
                    // ------------------------------------------------

                    let rng: f64 =
                        rand::thread_rng().gen();


                    /*
                     * Se rng < 0.15:
                     * il veicolo inizia una sosta.
                     *
                     * Altrimenti:
                     * cambia posizione.
                     */
                    if rng < 0.15 {

                        /*
                         * Ogni ciclo dura 30 secondi.
                         * 8 cicli = circa 4 minuti.
                         */
                        pause_counter = 8;

                    } else {

                        // Random walk
                        //TODO: valutare se ridurre il raggio

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


                // Creiamo il messaggio da inviare
                let msg =
                    Message::PositionUpdate {
                        user_id:
                        user_id_clone.clone(),

                        coords,

                        timestamp:
                        Utc::now(),
                    };


                /*
                 * Il GPS non scrive direttamente sulla TCP.
                 *
                 * Manda il messaggio al main.
                 */
                if tx_gps.send(msg).await.is_err() {
                    break;
                }


                // Una posizione ogni 30 secondi
                tokio::time::sleep(
                    Duration::from_secs(30)
                )
                    .await;
            }
        });


        // ============================================================
        // 3. INPUT CONSOLE
        // ============================================================

        // Copia dello user_id per il task console
        let user_id_cli = user_id.clone();


        /*
         * Secondo channel interno.
         *
         * console -> tx_cli -> rx_cli -> main -> server
         */
        let (tx_cli, mut rx_cli) =
            tokio::sync::mpsc::channel::<Message>(10);


        /*
         * Anche qui conserviamo il JoinHandle
         * per poter fermare il task a fine sessione.
         */
        let cli_handle = tokio::spawn(async move {

            let stdin =
                tokio::io::stdin();

            let mut reader =
                BufReader::new(stdin);

            let mut input =
                String::new();


            loop {

                input.clear();


                let bytes =
                    match reader
                        .read_line(&mut input)
                        .await
                    {
                        Ok(bytes) => bytes,

                        Err(e) => {
                            eprintln!(
                                "Errore lettura input: {}",
                                e
                            );

                            break;
                        }
                    };


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


                // ====================================================
                // LOGOUT
                // ====================================================

                if text == "/logout" {

                    let msg =
                        Message::LogoutRequest {
                            user_id:
                            user_id_cli.clone(),
                        };


                    /*
                     * Mandiamo LogoutRequest al main.
                     *
                     * Sarà il main ad inviarlo
                     * effettivamente sulla TCP.
                     */
                    if tx_cli.send(msg).await.is_err() {
                        break;
                    }


                    /*
                     * La console di questa sessione
                     * non deve più accettare comandi.
                     */
                    break;
                }


                // ====================================================
                // MESSAGGIO AL SERVER
                // ====================================================

                else if text.starts_with("/msg ") {

                    let msg_content =
                        text
                            .strip_prefix("/msg ")
                            .unwrap()
                            .trim();


                    /*
                     * Evitiamo anche:
                     *
                     * /msg
                     *
                     * senza testo.
                     */
                    if msg_content.is_empty() {

                        println!(
                            "Sistema: il messaggio non può essere vuoto."
                        );

                        continue;
                    }


                    let msg =
                        Message::ClientToServerText {
                            user_id:
                            user_id_cli.clone(),

                            content:
                            msg_content.to_string(),
                        };


                    if tx_cli.send(msg).await.is_err() {
                        break;
                    }
                }


                // ====================================================
                // COMANDO NON VALIDO
                // ====================================================

                else {

                    println!(
                        "Sistema: comandi disponibili:"
                    );

                    println!(
                        "/msg <testo>"
                    );

                    println!(
                        "/logout"
                    );
                }
            }
        });


        // ============================================================
        // 4. MAIN LOOP DELLA SESSIONE
        // ============================================================

        /*
         * Questo loop gestisce contemporaneamente:
         *
         * 1. server -> client
         * 2. GPS -> server
         * 3. console -> server
         */
        let mut line =
            String::new();


        /*
         * false:
         * sessione normale
         *
         * true:
         * LogoutRequest già inviato,
         * stiamo aspettando LogoutResponse.
         */
        let mut logout_requested =
            false;


        loop {

            line.clear();


            tokio::select! {


                // ====================================================
                // RICEZIONE DAL SERVER
                // ====================================================

                bytes_read =
                    reader.read_line(&mut line) => {

                    let bytes =
                        bytes_read?;


                    /*
                     * 0 byte significa che il server
                     * ha chiuso la connessione.
                     */
                    if bytes == 0 {

                        println!(
                            "Disconnesso dal server."
                        );

                        break;
                    }


                    /*
                     * Convertiamo il JSON ricevuto
                     * in Message.
                     */
                    let msg =
                        match serde_json
                            ::from_str::<Message>(&line)
                        {
                            Ok(msg) => msg,

                            Err(e) => {

                                eprintln!(
                                    "Messaggio non valido ricevuto dal server: {}",
                                    e
                                );

                                continue;
                            }
                        };


                    match msg {


                        // --------------------------------------------
                        // BROADCAST
                        // --------------------------------------------

                        Message::ServerToClientBroadcast {
                            content
                        } => {

                            println!(
                                "[BROADCAST]: {}",
                                content
                            );
                        }


                        // --------------------------------------------
                        // MESSAGGIO DIRETTO
                        // --------------------------------------------

                        Message::ServerToClientDirect {
                            target_user_id: _,
                            content
                        } => {

                            println!(
                                "[MESSAGGIO]: {}",
                                content
                            );
                        }


                        // --------------------------------------------
                        // RISPOSTA AL LOGOUT
                        // --------------------------------------------

                        Message::LogoutResponse {
                            success,
                            message,
                        } => {

                            println!(
                                "Server: {}",
                                message
                            );


                            if success {

                                /*
                                 * Il server ha confermato
                                 * di aver eliminato la sessione.
                                 *
                                 * Possiamo uscire dal loop
                                 * della sessione.
                                 */
                                break;

                            } else {

                                /*
                                 * Il server non ha effettuato
                                 * il logout.
                                 */
                                logout_requested =
                                    false;

                                println!(
                                    "Logout non riuscito."
                                );
                            }
                        }


                        // --------------------------------------------
                        // ALTRI MESSAGGI
                        // --------------------------------------------

                        _ => {}
                    }
                }


                // ====================================================
                // INVIO GPS AL SERVER
                // ====================================================

                /*
                 * Questo ramo viene eseguito solamente
                 * se NON abbiamo richiesto il logout.
                 */
                Some(msg) = rx_gps.recv(),
                if !logout_requested => {

                    let json =
                        serde_json::to_string(&msg)?
                        + "\n";


                    write_half
                        .write_all(
                            json.as_bytes()
                        )
                        .await?;
                }


                // ====================================================
                // INVIO COMANDI CONSOLE AL SERVER
                // ====================================================

                Some(msg) = rx_cli.recv(),
                if !logout_requested => {

                    /*
                     * Controlliamo se il messaggio
                     * ricevuto dalla console è
                     * LogoutRequest.
                     */
                    let is_logout =
                        matches!(
                            &msg,
                            Message::LogoutRequest { .. }
                        );


                    // Convertiamo in JSON
                    let json =
                        serde_json::to_string(&msg)?
                        + "\n";


                    // Invio sulla TCP
                    write_half
                        .write_all(
                            json.as_bytes()
                        )
                        .await?;


                    if is_logout {

                        /*
                         * ATTENZIONE:
                         *
                         * qui NON facciamo break.
                         *
                         * Abbiamo solamente inviato
                         * LogoutRequest.
                         *
                         * Aspettiamo la conferma
                         * LogoutResponse dal server.
                         */
                        logout_requested =
                            true;

                        println!(
                            "Logout in corso..."
                        );
                    }
                }
            }
        }


        // ============================================================
        // 5. FINE DELLA SESSIONE
        // ============================================================

        /*
         * Fermiamo i task appartenenti
         * alla vecchia sessione.
         */
        gps_handle.abort();
        cli_handle.abort();


        /*
         * Eliminiamo le due metà della connessione TCP.
         *
         * In questo modo la vecchia connessione
         * viene chiusa.
         */
        drop(write_half);
        drop(reader);


        println!(
            "Sessione terminata.\n"
        );


        /*
         * NON facciamo return.
         *
         * Siamo dentro il loop esterno.
         *
         * Quindi da qui si torna automaticamente
         * a:
         *
         * auth::authenticate().await?
         *
         * e viene mostrato nuovamente:
         *
         * === GeoRuggine ===
         * 1) Login
         * 2) Registrazione
         */
    }
}