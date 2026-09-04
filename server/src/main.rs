mod analysis;
mod auth;
mod cli;
mod connection;
mod db;
mod state;
mod tasks;

use std::collections::HashMap;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::sync::RwLock;

use db::init_db;

use state::{
    ServerState,
    SharedState,
};


#[tokio::main]
async fn main()
    -> Result<(), Box<dyn std::error::Error>>
{
    // ========================================================
    // DATABASE
    // ========================================================

    let db_pool =
        init_db()
            .expect(
                "Impossibile inizializzare il database"
            );


    // ========================================================
    // STATO SERVER
    // ========================================================

    let state_data = ServerState {
        clients: RwLock::new(HashMap::new()),
        db_pool,
    };

    let state: SharedState = Arc::new(state_data);


    // ========================================================
    // SERVER TCP
    // ========================================================

    let listener =
        TcpListener::bind(
            "127.0.0.1:8080"
        )
            .await?;


    println!(
        "Server in ascolto su 127.0.0.1:8080"
    );


    // ========================================================
    // TASK MONITORAGGIO STATO
    // ========================================================

    let state_for_monitor =
        state.clone();

    tokio::spawn(async move {
        tasks::state_monitor_task(
            state_for_monitor
        )
            .await;
    });


    // ========================================================
    // CLI SERVER
    // ========================================================

    let state_for_commands =
        state.clone();

    tokio::spawn(async move {
        cli::command_loop(
            state_for_commands
        )
            .await;
    });


    // ========================================================
    // LOGGER CPU
    // ========================================================

    tokio::spawn(async move {
        tasks::cpu_logger_task().await;
    });


    // ========================================================
    // ACCETTAZIONE CLIENT
    // ========================================================

    loop {
        let (socket, _) =
            listener.accept().await?;


        let state_for_client =
            state.clone();


        tokio::spawn(async move {

            if let Err(e) =
                connection::handle_client(
                    socket,
                    state_for_client,
                )
                    .await
            {
                eprintln!(
                    "Errore gestione client: {}",
                    e
                );
            }
        });
    }
}