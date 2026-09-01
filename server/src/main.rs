mod analysis;
mod auth;

use auth::{hash_password, load_accounts, save_accounts, verify_password};

use chrono::{DateTime, Utc};

use shared::{Coordinates, Message, UserId, UserState};

use std::collections::HashMap;
use std::sync::Arc;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

use tokio::net::{TcpListener, TcpStream};

use tokio::sync::{mpsc, RwLock};

use std::fs::OpenOptions;
use std::io::Write;

use sysinfo::{get_current_pid, ProcessExt, System, SystemExt};

// ============================================================
// DATI CLIENT
// ============================================================

#[derive(Debug, Clone)]
struct ClientData {
    username: String,
    state: UserState,
    last_position: Option<Coordinates>,
    last_move_time: Option<DateTime<Utc>>,
    state_history: Vec<(UserState, DateTime<Utc>)>,
    distance_history: Vec<(f64, DateTime<Utc>)>,
    sender: mpsc::Sender<Message>,
}

#[derive(Debug, Clone, Default)]
struct UserHistory {
    state_history: Vec<(UserState, DateTime<Utc>)>,
    distance_history: Vec<(f64, DateTime<Utc>)>,
}

// ============================================================
// STATO SERVER
// ============================================================

struct ServerState {
    clients: HashMap<UserId, ClientData>,
    accounts: HashMap<String, String>, // username -> password hash
    histories: HashMap<String, UserHistory>,
}

// FIXME: rivedi gestione lock
type SharedState = Arc<RwLock<ServerState>>;

// ============================================================
// MAIN
// ============================================================

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let state_data = ServerState {
        clients: HashMap::new(),
        accounts: load_accounts(),
        histories: HashMap::new(),
    };

    let state: SharedState = Arc::new(RwLock::new(state_data));

    let listener = TcpListener::bind("127.0.0.1:8080").await?;

    println!("Server in ascolto su 127.0.0.1:8080");

    // ========================================================
    // TASK MONITORAGGIO STATO UTENTI
    // ========================================================

    let state_clone = state.clone();

    tokio::spawn(async move {
        state_monitor_task(state_clone).await;
    });

    // ========================================================
    // TASK INPUT CONSOLE SERVER
    // ========================================================

    let state_for_stdin = state.clone();

    tokio::spawn(async move {
        let stdin = tokio::io::stdin();

        let mut reader = tokio::io::BufReader::new(stdin);

        let mut input = String::new();

        loop {
            input.clear();

            if let Ok(bytes) = reader.read_line(&mut input).await {
                if bytes == 0 {
                    break;
                }

                let text = input.trim();

                if text.is_empty() {
                    continue;
                }

                // =================================================
                // MESSAGGIO PRIVATO
                // =================================================

                if text.starts_with("/msg ") {
                    let parts: Vec<&str> = text.splitn(3, ' ').collect();

                    if parts.len() == 3 {
                        let target_name = parts[1];

                        let msg_content = parts[2];

                        let r_state = state_for_stdin.read().await;

                        // FIXME: cambia nome del campo userid
                        let direct_msg = Message::ServerToClientDirect {
                            target_user_id: "Server".to_string(),

                            content: msg_content.to_string(),
                        };

                        let mut found = false;

                        for (_, client) in r_state.clients.iter() {
                            if client.username == target_name {
                                let _ = client.sender.send(direct_msg.clone()).await;

                                found = true;
                            }
                        }

                        if found {
                            println!("Messaggio privato inviato a {}", target_name);
                        } else {
                            println!("Utente {} non trovato", target_name);
                        }
                    } else {
                        println!("Uso corretto: /msg <utente> <messaggio>");
                    }
                }
                // =================================================
                // STATISTICHE
                // =================================================
                else if text.starts_with("/stats") {
                    let parts: Vec<&str> = text.split_whitespace().collect();

                    if parts.len() == 3 {
                        let target_name = parts[1];

                        let interval = parts[2];

                        if interval != "giorno"
                            && interval != "settimana"
                            && interval != "mese"
                            && interval != "all"
                        {
                            println!(
                                "Intervallo '{}' non valido. \
                                 Usa: giorno, settimana, mese, all",
                                interval
                            );

                            continue;
                        }

                        let end_time = chrono::Utc::now();

                        let start_time = match interval {
                            "giorno" => {
                                use chrono::{Datelike, TimeZone};

                                chrono::Utc
                                    .with_ymd_and_hms(
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
                                use chrono::{Datelike, TimeZone};

                                let days_from_monday = end_time.weekday().num_days_from_monday();

                                let monday =
                                    end_time - chrono::Duration::days(days_from_monday as i64);

                                chrono::Utc
                                    .with_ymd_and_hms(
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
                                use chrono::{Datelike, TimeZone};

                                chrono::Utc
                                    .with_ymd_and_hms(end_time.year(), end_time.month(), 1, 0, 0, 0)
                                    .unwrap()
                            }

                            _ => chrono::DateTime::<Utc>::MIN_UTC,
                        };

                        // ricerca dello storico

                        let r_state = state_for_stdin.read().await;

                        let mut found = false;


                        // =====================================================
                        // 1. CERCHIAMO TRA GLI UTENTI CONNESSI
                        // =====================================================

                        for (_, client) in r_state.clients.iter() {

                            if client.username == target_name {

                                found = true;


                                let result = analysis::analyze_movement(
                                    &client.state_history,
                                    &client.distance_history,
                                    start_time,
                                    end_time,
                                );


                                let state_str = match client.state {

                                    UserState::Fermo =>
                                        "Fermo",

                                    UserState::InMovimento =>
                                        "In Movimento",

                                    UserState::Disconnesso =>
                                        "Sconnesso",
                                };


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


                                break;
                            }
                        }


                        // =====================================================
                        // 2. SE NON È CONNESSO, CERCHIAMO NELLO STORICO
                        // =====================================================

                        if !found {

                            if let Some(history) =
                                r_state.histories.get(target_name)
                            {

                                found = true;


                                let result = analysis::analyze_movement(
                                    &history.state_history,
                                    &history.distance_history,
                                    start_time,
                                    end_time,
                                );


                                println!(
                                    "=== STATISTICHE per {} ({}) ===\n\
                                     Stato Attuale: Sconnesso\n\
                                     Distanza: {:.2} km\n\
                                     Velocità Media: {:.2} km/h\n\
                                     Tempo in Movimento: {} sec\n\
                                     Tempo Pause: {} sec\n\
                                     ===============================",
                                    target_name,
                                    interval,
                                    result.total_distance_km,
                                    result.average_speed_kmh,
                                    result.moving_time_secs,
                                    result.pause_time_secs
                                );
                            }
                        }


                        // =====================================================
                        // 3. UTENTE MAI TROVATO
                        // =====================================================

                        if !found {

                            println!(
                                "Utente {} non trovato.",
                                target_name
                            );
                        }
                    } else {
                        println!(
                            "Uso corretto: \
                             /stats <utente> \
                             <giorno|settimana|mese|all>"
                        );
                    }
                }
                // =================================================
                // BROADCAST
                // =================================================
                else if text.starts_with("/b ") {
                    let msg_content = text.strip_prefix("/b ").unwrap().trim();

                    let broadcast_msg = Message::ServerToClientBroadcast {
                        content: msg_content.to_string(),
                    };

                    let r_state = state_for_stdin.read().await;

                    for (_, client) in r_state.clients.iter() {
                        let _ = client.sender.send(broadcast_msg.clone()).await;
                    }

                    println!("Messaggio broadcast inviato a tutti");
                }
                // =================================================
                // MENU
                // =================================================
                else {
                    println!("--- Menu Comandi Server ---");

                    println!(
                        "/msg <utente> <testo>  : \
                         Invia un messaggio privato a un utente"
                    );

                    println!(
                        "/b <testo>             : \
                         Invia un messaggio broadcast a tutti"
                    );

                    println!(
                        "/stats <utente> <int.> : \
                         Mostra le statistiche \
                         (all, giorno, settimana, mese)"
                    );

                    println!("---------------------------");
                }
            }
        }
    });

    // ========================================================
    // TASK LOGGER CPU
    // ========================================================

    tokio::spawn(async move {
        cpu_logger_task().await;
    });

    // ========================================================
    // ACCETTAZIONE CLIENT
    // ========================================================

    loop {
        let (socket, _) = listener.accept().await?;

        let state_clone = state.clone();

        tokio::spawn(async move {
            if let Err(e) = handle_client(socket, state_clone).await {
                eprintln!("Errore client: {}", e);
            }
        });
    }
}

// ============================================================
// GESTIONE CLIENT
// ============================================================

async fn handle_client(
    mut socket: TcpStream,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error>> {
    let (read_half, mut write_half) = socket.split();

    let mut reader = BufReader::new(read_half);

    let mut line = String::new();

    /*
     * Contiene lo user_id dell'utente
     * autenticato su questa connessione.
     *
     * None = nessun utente autenticato.
     */
    let mut current_user_id: Option<UserId> = None;

    /*
     * Channel utilizzato dal server
     * per inviare messaggi al client.
     */
    let (tx, mut rx) = mpsc::channel::<Message>(32);

    // ========================================================
    // LOOP DELLA CONNESSIONE
    // ========================================================

    loop {
        line.clear();

        tokio::select! {


                    // =================================================
                    // MESSAGGI RICEVUTI DAL CLIENT
                    // =================================================

                    bytes_read =
                        reader.read_line(&mut line) => {

                        let bytes_read =
                            bytes_read?;


                        /*
                         * EOF:
                         * il client ha chiuso la connessione.
                         */
                        if bytes_read == 0 {
                            break;
                        }


                        let msg: Message =
                            match serde_json::from_str(&line) {

                                Ok(m) =>
                                    m,

                                Err(e) => {

                                    eprintln!(
                                        "Errore parsing JSON: {}",
                                        e
                                    );

                                    continue;
                                }
                            };


                        match msg {


                            // =========================================
                            // REGISTRAZIONE
                            // =========================================

                            Message::RegisterRequest {
                                username,
                                password,
                            } => {

                                let mut w_state =
                                    state.write().await;


                                if w_state
                                    .accounts
                                    .contains_key(&username)
                                {

                                    let response =
                                        Message::RegisterResponse {
                                            success: false,
                                            message:
                                                "Utente già esistente"
                                                    .to_string(),
                                        };


                                    let response_json =
                                        serde_json::to_string(
                                            &response
                                        )? + "\n";


                                    write_half
                                        .write_all(
                                            response_json.as_bytes()
                                        )
                                        .await?;

                                } else {

                                    let hashed_password =
                                        hash_password(
                                            &password
                                        )?;


                                    w_state
                                        .accounts
                                        .insert(
                                            username.clone(),
                                            hashed_password,
                                        );


                                    save_accounts(
                                        &w_state.accounts
                                    )?;


                                    let response =
                                        Message::RegisterResponse {
                                            success: true,
                                            message:
                                                "Registrazione completata"
                                                    .to_string(),
                                        };


                                    let response_json =
                                        serde_json::to_string(
                                            &response
                                        )? + "\n";


                                    write_half
                                        .write_all(
                                            response_json.as_bytes()
                                        )
                                        .await?;


                                    println!(
                                        "Nuovo utente registrato: {}",
                                        username
                                    );
                                }
                            }


                            // =========================================
                            // LOGIN
                            // =========================================

                            Message::LoginRequest {
                                username,
                                password,
                            } => {

                                let mut w_state =
                                    state.write().await;


                                /*
                                 * Impediamo il doppio login.
                                 *
                                 * clients contiene solamente
                                 * le sessioni attualmente attive.
                                 */
                                let is_already_connected =
                                    w_state
                                        .clients
                                        .values()
                                        .any(
                                            |c|
                                            c.username == username
                                        );


                                if is_already_connected {

                                    let response =
                                        Message::LoginResponse {
                                            success: false,
                                            user_id: None,
                                            message:
                                                "Utente già connesso \
                                                 su un altro dispositivo"
                                                    .to_string(),
                                        };


                                    let response_json =
                                        serde_json::to_string(
                                            &response
                                        )? + "\n";


                                    write_half
                                        .write_all(
                                            response_json.as_bytes()
                                        )
                                        .await?;


                                    continue;
                                }


                                if let Some(stored_pwd) =
                                    w_state.accounts.get(&username)
                                {

                                    if verify_password(
                                        &password,
                                        stored_pwd,
                                    ) {

                                        /*
                                         * Creiamo un nuovo ID
                                         * per questa sessione.
                                         */
                                        let user_id =uuid::Uuid::new_v4().to_string();


                                        current_user_id = Some(user_id.clone());

                                        let previous_history = w_state
                                            .histories
                                            .remove(&username)
                                            .unwrap_or_default();


                                        let mut state_history = previous_history.state_history;

                                        state_history.push(
                                            (UserState::Fermo, Utc::now())
                                        );


                                        let client_data = ClientData {
                                            username: username.clone(),

                                            state: UserState::Fermo,

                                            last_position: None,

                                            last_move_time: None,

                                            state_history,

                                            distance_history:
                                                previous_history.distance_history,

                                            sender: tx.clone(),
                                        };


                                        w_state
                                            .clients
                                            .insert(
                                                user_id.clone(),
                                                client_data,
                                            );


                                        let response =
                                            Message::LoginResponse {
                                                success: true,
                                                user_id:
                                                    Some(
                                                        user_id.clone()
                                                    ),
                                                message:
                                                    format!(
                                                        "Benvenuto {}",
                                                        username
                                                    ),
                                            };


                                        let response_json =
                                            serde_json::to_string(
                                                &response
                                            )? + "\n";


                                        write_half
                                            .write_all(
                                                response_json.as_bytes()
                                            )
                                            .await?;


                                        println!(
                                            "Utente {} autenticato \
                                             con ID {}",
                                            username,
                                            user_id
                                        );

                                    } else {

                                        let response =
                                            Message::LoginResponse {
                                                success: false,
                                                user_id: None,
                                                message:
                                                    "Password errata"
                                                        .to_string(),
                                            };


                                        let response_json =
                                            serde_json::to_string(
                                                &response
                                            )? + "\n";


                                        write_half
                                            .write_all(
                                                response_json.as_bytes()
                                            )
                                            .await?;
                                    }

                                } else {

                                    let response =
                                        Message::LoginResponse {
                                            success: false,
                                            user_id: None,
                                            message:
                                                "Utente non trovato"
                                                    .to_string(),
                                        };


                                    let response_json =
                                        serde_json::to_string(
                                            &response
                                        )? + "\n";


                                    write_half
                                        .write_all(
                                            response_json.as_bytes()
                                        )
                                        .await?;
                                }
                            }


                            // =========================================
                            // AGGIORNAMENTO POSIZIONE
                            // =========================================

                            Message::PositionUpdate {
                                user_id,
                                coords,
                                timestamp,
                            } => {

                                /*
                                 * Accettiamo PositionUpdate solamente
                                 * dall'utente autenticato
                                 * su questa connessione.
                                 */
                                if Some(user_id.clone())
                                    == current_user_id
                                {

                                    let mut w_state =
                                        state.write().await;


                                    if let Some(client) =
                                        w_state
                                            .clients
                                            .get_mut(&user_id)
                                    {

                                        if let Some(last_pos) =
                                            &client.last_position
                                        {

                                            let dist =
                                                crate::analysis
                                                    ::calculate_distance(
                                                        last_pos,
                                                        &coords,
                                                    );


                                            client
                                                .distance_history
                                                .push(
                                                    (
                                                        dist,
                                                        timestamp,
                                                    )
                                                );


                                            if dist > 0.001 {

                                                if client.state
                                                    != UserState::InMovimento
                                                {

                                                    client.state =
                                                        UserState::InMovimento;


                                                    client
                                                        .state_history
                                                        .push(
                                                            (
                                                                UserState::InMovimento,
                                                                timestamp,
                                                            )
                                                        );


                                                    println!(
                                                        "Utente {} è ora \
                                                         in stato: In Movimento",
                                                        client.username
                                                    );
                                                }


                                                client.last_move_time =
                                                    Some(timestamp);
                                            }

                                        } else {

                                            // Prima posizione
                                            client.last_move_time =
                                                Some(timestamp);
                                        }


                                        client.last_position =
                                            Some(coords.clone());
                                    }
                                }
                            }


                            // =========================================
                            // MESSAGGIO CLIENT -> SERVER
                            // =========================================

                            Message::ClientToServerText {
                                user_id,
                                content,
                            } => {

                                let sender_name = {

                                    let r_state =
                                        state.read().await;


                                    r_state
                                        .clients
                                        .get(&user_id)
                                        .map(
                                            |c|
                                            c.username.clone()
                                        )
                                        .unwrap_or_else(
                                            ||
                                            user_id.clone()
                                        )
                                };


                                println!(
                                    "Messaggio da {}: {}",
                                    sender_name,
                                    content
                                );
                            }


                            // =========================================
                            // LOGOUT
                            // =========================================

                            Message::LogoutRequest {
                                user_id
                            } => {

                                /*
                                 * Controlliamo che lo user_id
                                 * della richiesta sia quello
                                 * autenticato su questa connessione.
                                 */
                                if current_user_id.as_ref()
                                    == Some(&user_id)
                                {

                                    /*
                                     * Rimuoviamo realmente
                                     * la sessione dalla HashMap
                                     * degli utenti connessi.
                                     */
                                    let username = {

                                        let mut w_state =
                                            state.write().await;

                                        if let Some(mut client) =
                                            w_state.clients.remove(&user_id)
                                        {

                                            /*
                                             * Registriamo anche il momento
                                             * della disconnessione.
                                             */
                                            client.state =
                                                UserState::Disconnesso;

                                            client.state_history.push(
                                                (
                                                    UserState::Disconnesso,
                                                    Utc::now(),
                                                )
                                            );


                                            let username =
                                                client.username.clone();


                                            /*
                                             * Salviamo lo storico separatamente
                                             * dalla sessione.
                                             */
                                            w_state.histories.insert(
                                                username.clone(),

                                                UserHistory {
                                                    state_history:
                                                        client.state_history,

                                                    distance_history:
                                                        client.distance_history,
                                                },
                                            );


                                            Some(username)

                                        } else {

                                            None
                                        }
                                    };


                                    /*
                                     * Confermiamo al client
                                     * che il logout è riuscito.
                                     */
                                    let response =
                                        Message::LogoutResponse {
                                            success: true,
                                            message:
                                                "Logout effettuato correttamente"
                                                    .to_string(),
                                        };


                                    let response_json =
                                        serde_json::to_string(
                                            &response
                                        )? + "\n";


                                    write_half
                                        .write_all(
                                            response_json.as_bytes()
                                        )
                                        .await?;


                                    if let Some(username) =
                                        username
                                    {

                                        println!(
                                            "Utente {} disconnesso",
                                            username
                                        );
                                    }


                                    /*
                                     * La sessione è già stata
                                     * eliminata dalla HashMap.
                                     *
                                     * Mettiamo None così il cleanup
                                     * finale non tenta di eliminarla
                                     * una seconda volta.
                                     */
                                    current_user_id =
                                        None;


                                    /*
                                     * Terminiamo il loop che gestisce
                                     * questa specifica connessione TCP.
                                     */
                                    break;

                                } else {

                                    /*
                                     * Lo user_id ricevuto non coincide
                                     * con quello della sessione.
                                     */
                                    let response =
                                        Message::LogoutResponse {
                                            success: false,
                                            message:
                                                "Sessione non valida"
                                                    .to_string(),
                                        };


                                    let response_json =
                                        serde_json::to_string(
                                            &response
                                        )? + "\n";


                                    write_half
                                        .write_all(
                                            response_json.as_bytes()
                                        )
                                        .await?;
                                }
                            }


                            // =========================================
                            // ALTRI MESSAGGI
                            // =========================================

                            _ => {}
                        }
                    }


                    // =================================================
                    // MESSAGGI SERVER -> CLIENT
                    // =================================================

                    Some(out_msg) =
                        rx.recv() => {

                        let json =
                            serde_json::to_string(
                                &out_msg
                            )? + "\n";


                        write_half
                            .write_all(
                                json.as_bytes()
                            )
                            .await?;
                    }
                }
    }

    // ========================================================
    // CLEANUP DISCONNESSIONE IMPREVISTA
    // ========================================================
    //
    // Questo blocco serve quando il client NON usa /logout,
    // ad esempio:
    //
    // - chiude il programma
    // - perde la connessione
    // - crasha
    //
    // Se invece ha fatto /logout,
    // current_user_id è già None.
    // in questo modo salviamo anche lo storico

    if let Some(user_id) = current_user_id {

        let mut w_state =
            state.write().await;


        if let Some(mut client) =
            w_state.clients.remove(&user_id)
        {

            client.state =
                UserState::Disconnesso;


            client.state_history.push(
                (
                    UserState::Disconnesso,
                    Utc::now(),
                )
            );


            let username =
                client.username.clone();


            w_state.histories.insert(
                username.clone(),

                UserHistory {
                    state_history:
                    client.state_history,

                    distance_history:
                    client.distance_history,
                },
            );


            println!(
                "Utente {} disconnesso",
                username
            );
        }
    }

    Ok(())
}

// ============================================================
// MONITORAGGIO STATO UTENTI
// ============================================================

async fn state_monitor_task(state: SharedState) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(30));

    loop {
        interval.tick().await;

        let now = Utc::now();

        let mut w_state = state.write().await;

        for (_, client) in w_state.clients.iter_mut() {
            if client.state == UserState::InMovimento {
                if let Some(last_time) = client.last_move_time {
                    /*
                     * Se non ci sono aggiornamenti
                     * di movimento per 3 minuti,
                     * passa allo stato Fermo.
                     */
                    if now.signed_duration_since(last_time).num_minutes() >= 3 {
                        client.state = UserState::Fermo;

                        client.state_history.push((UserState::Fermo, now));

                        println!(
                            "Utente {} passato a stato \
                             Fermo per inattività",
                            client.username
                        );
                    }
                }
            }
        }
    }
}

// ============================================================
// CPU LOGGER
// ============================================================

async fn cpu_logger_task() {
    let mut sys = System::new();

    let pid = get_current_pid().unwrap();

    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(120));

    // Primo refresh
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
