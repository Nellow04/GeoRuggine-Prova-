use chrono::Utc;

use shared::{
    Message,
    UserId,
    UserState,
};

use tokio::io::{
    AsyncBufReadExt,
    AsyncWriteExt,
    BufReader,
};

use tokio::net::TcpStream;
use tokio::sync::mpsc;

use crate::analysis;
use crate::auth::{
    hash_password,
    verify_password,
};

use crate::db;

use crate::state::{
    ClientData,
    SharedState,
};


// ============================================================
// GESTIONE CLIENT
// ============================================================

pub async fn handle_client(
    mut socket: TcpStream,
    state: SharedState,
) -> Result<(), Box<dyn std::error::Error>> {

    let (read_half, mut write_half) =
        socket.split();

    let mut reader =
        BufReader::new(read_half);

    let mut line = String::new();

    let mut current_user_id: Option<UserId> = None;

    let (tx, mut rx) =
        mpsc::channel::<Message>(32);


    loop {
        line.clear();

        tokio::select! {

            // =================================================
            // MESSAGGIO RICEVUTO DAL CLIENT
            // =================================================

            bytes_read = reader.read_line(&mut line) => {

                let bytes_read = match bytes_read {
                    Ok(b) => b,

                    Err(e) => {
                        eprintln!(
                            "Errore lettura client: {}",
                            e
                        );

                        break;
                    }
                };


                if bytes_read == 0 {
                    break;
                }


                let msg: Message =
                    match serde_json::from_str(&line) {

                        Ok(m) => m,

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
                        password
                    } => {

                        let w_state =
                            state.write().await;


                        let hashed_password =
                            match hash_password(&password) {

                                Ok(h) => h,

                                Err(e) => {
                                    eprintln!(
                                        "Errore hash password: {}",
                                        e
                                    );

                                    continue;
                                }
                            };


                        let new_id =
                            uuid::Uuid::new_v4()
                                .to_string();


                        match db::register_user(
                            &w_state.db_pool,
                            &new_id,
                            &username,
                            &hashed_password,
                        ) {

                            Ok(true) => {

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


                                if write_half
                                    .write_all(
                                        response_json.as_bytes()
                                    )
                                    .await
                                    .is_err()
                                {
                                    break;
                                }


                                println!(
                                    "Nuovo utente registrato: {}",
                                    username
                                );
                            }


                            Ok(false) => {

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


                                if write_half
                                    .write_all(
                                        response_json.as_bytes()
                                    )
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }


                            Err(e) => {
                                eprintln!(
                                    "Errore DB in registrazione: {}",
                                    e
                                );
                            }
                        }
                    },


                    // =========================================
                    // LOGIN
                    // =========================================

                    Message::LoginRequest {
                        username,
                        password
                    } => {

                        let mut w_state =
                            state.write().await;


                        let is_already_connected =
                            w_state
                                .clients
                                .values()
                                .any(|c| {
                                    c.username == username
                                        && c.state
                                            != UserState::Disconnesso
                                });


                        if is_already_connected {

                            let response =
                                Message::LoginResponse {
                                    success: false,

                                    user_id: None,

                                    message:
                                        "Utente già connesso su un altro dispositivo"
                                            .to_string(),
                                };


                            let response_json =
                                serde_json::to_string(
                                    &response
                                )? + "\n";


                            if write_half
                                .write_all(
                                    response_json.as_bytes()
                                )
                                .await
                                .is_err()
                            {
                                break;
                            }


                            continue;
                        }


                        match db::get_user_by_name(
                            &w_state.db_pool,
                            &username,
                        ) {

                            Ok(Some((db_id, db_hash))) => {

                                if verify_password(
                                    &password,
                                    &db_hash,
                                ) {

                                    current_user_id =
                                        Some(db_id.clone());

                                    let now =
                                        Utc::now();


                                    let client_data =
                                        ClientData {
                                            username:
                                                username.clone(),

                                            state:
                                                UserState::Fermo,

                                            last_position:
                                                None,

                                            last_move_time:
                                                None,

                                            state_history:
                                                vec![(
                                                    UserState::Fermo,
                                                    now
                                                )],

                                            distance_history:
                                                Vec::new(),

                                            sender:
                                                tx.clone(),
                                        };


                                    let _ =
                                        db::insert_state(
                                            &w_state.db_pool,
                                            &db_id,
                                            "Fermo",
                                            now,
                                        );


                                    w_state
                                        .clients
                                        .insert(
                                            db_id.clone(),
                                            client_data,
                                        );


                                    let response =
                                        Message::LoginResponse {
                                            success: true,

                                            user_id:
                                                Some(
                                                    db_id.clone()
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


                                    if write_half
                                        .write_all(
                                            response_json.as_bytes()
                                        )
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }


                                    println!(
                                        "Utente {} autenticato.",
                                        username
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


                                    if write_half
                                        .write_all(
                                            response_json.as_bytes()
                                        )
                                        .await
                                        .is_err()
                                    {
                                        break;
                                    }
                                }
                            }


                            Ok(None) => {

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


                                if write_half
                                    .write_all(
                                        response_json.as_bytes()
                                    )
                                    .await
                                    .is_err()
                                {
                                    break;
                                }
                            }


                            Err(e) => {
                                eprintln!(
                                    "Errore DB in login: {}",
                                    e
                                );
                            }
                        }
                    },


                    // =========================================
                    // LOGOUT
                    // =========================================

                    Message::LogoutRequest {
                        user_id
                    } => {

                        if current_user_id.as_ref()
                            == Some(&user_id)
                        {
                            let mut w_state =
                                state.write().await;


                            let username =
                                if let Some(client) =
                                    w_state
                                        .clients
                                        .remove(&user_id)
                                {
                                    let now =
                                        Utc::now();


                                    let _ =
                                        db::insert_state(
                                            &w_state.db_pool,
                                            &user_id,
                                            "Disconnesso",
                                            now,
                                        );


                                    client.username

                                } else {
                                    user_id.clone()
                                };


                            let response =
                                Message::LogoutResponse {
                                    success: true,

                                    message:
                                        "Logout effettuato con successo"
                                            .to_string(),
                                };


                            let response_json =
                                serde_json::to_string(
                                    &response
                                )? + "\n";


                            let _ =
                                write_half
                                    .write_all(
                                        response_json.as_bytes()
                                    )
                                    .await;


                            println!(
                                "Utente {} ha effettuato il logout",
                                username
                            );


                            current_user_id = None;

                        } else {

                            let response =
                                Message::LogoutResponse {
                                    success: false,

                                    message:
                                        "Utente non autorizzato al logout"
                                            .to_string(),
                                };


                            let response_json =
                                serde_json::to_string(
                                    &response
                                )? + "\n";


                            let _ =
                                write_half
                                    .write_all(
                                        response_json.as_bytes()
                                    )
                                    .await;
                        }
                    },


                    // =========================================
                    // AGGIORNAMENTO POSIZIONE
                    // =========================================

                    Message::PositionUpdate {
                        user_id,
                        coords,
                        timestamp
                    } => {

                        if Some(user_id.clone())
                            == current_user_id
                        {
                            let mut w_state =
                                state.write().await;


                            let pool =
                                w_state
                                    .db_pool
                                    .clone();


                            if let Some(client) =
                                w_state
                                    .clients
                                    .get_mut(&user_id)
                            {

                                if let Some(last_pos) =
                                    &client.last_position
                                {

                                    let dist =
                                        analysis::calculate_distance(
                                            last_pos,
                                            &coords,
                                        );


                                    client
                                        .distance_history
                                        .push((
                                            dist,
                                            timestamp
                                        ));


                                    let _ =
                                        db::insert_distance(
                                            &pool,
                                            &user_id,
                                            dist,
                                            timestamp,
                                        );


                                    if dist > 0.001 {

                                        if client.state
                                            != UserState::InMovimento
                                        {
                                            client.state =
                                                UserState::InMovimento;


                                            client
                                                .state_history
                                                .push((
                                                    UserState::InMovimento,
                                                    timestamp
                                                ));


                                            let _ =
                                                db::insert_state(
                                                    &pool,
                                                    &user_id,
                                                    "In Movimento",
                                                    timestamp,
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
                    },


                    // =========================================
                    // MESSAGGIO CLIENT -> SERVER
                    // =========================================

                    Message::ClientToServerText {
                        user_id,
                        content
                    } => {

                        let w_state =
                            state.write().await;


                        let sender_name =
                            w_state
                                .clients
                                .get(&user_id)
                                .map(|c| {
                                    c.username.clone()
                                })
                                .unwrap_or_else(|| {
                                    user_id.clone()
                                });


                        let _ =
                            db::insert_chat(
                                &w_state.db_pool,
                                &user_id,
                                Some("Server"),
                                &content,
                                Utc::now(),
                            );


                        println!(
                            "[Messaggio da {}]: {}",
                            sender_name,
                            content
                        );
                    },


                    _ => {}
                }
            }


            // =================================================
            // MESSAGGIO SERVER -> CLIENT
            // =================================================

            Some(out_msg) = rx.recv() => {

                let json =
                    serde_json::to_string(
                        &out_msg
                    )? + "\n";


                if write_half
                    .write_all(
                        json.as_bytes()
                    )
                    .await
                    .is_err()
                {
                    break;
                }
            }
        }
    }


    // =========================================================
    // DISCONNESSIONE IMPREVISTA
    // =========================================================

    if let Some(user_id) = current_user_id {

        let mut w_state =
            state.write().await;


        let pool =
            w_state
                .db_pool
                .clone();


        if let Some(client) =
            w_state
                .clients
                .get_mut(&user_id)
        {

            client.state =
                UserState::Disconnesso;


            let now =
                Utc::now();


            client
                .state_history
                .push((
                    UserState::Disconnesso,
                    now,
                ));


            let _ =
                db::insert_state(
                    &pool,
                    &user_id,
                    "Disconnesso",
                    now,
                );


            println!(
                "Utente {} disconnesso.",
                client.username
            );
        }
    }


    Ok(())
}