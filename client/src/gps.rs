use std::time::Duration;
use chrono::Utc;
use rand::Rng;
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use shared::{Coordinates, Message};

/// Intervallo tra un invio di posizione e il successivo (in secondi).
const UPDATE_INTERVAL_SECS: u64 = 30;

/// Probabilità che il veicolo effettui una sosta (15%).
const PAUSE_PROBABILITY: f64 = 0.15;

/// Numero di cicli di sosta (8 cicli * 30s = circa 4 minuti di sosta).
const PAUSE_CYCLES: usize = 8;

/// Avvia in background il task asincrono che simula il sensore GPS del veicolo.
///
/// Il task genera periodicamente un messaggio `Message::PositionUpdate` e lo inoltra
/// al canale interno `tx_gps`. In caso di sosta, le coordinate rimangono invariate;
/// altrimenti viene applicato un movimento casuale (random walk).
pub fn spawn_gps_task(user_id: String, tx_gps: Sender<Message>) -> JoinHandle<()> {
    tokio::spawn(async move {
        // Coordinate di partenza (Torino e dintorni)
        let mut lat = 45.0;
        let mut lon = 7.0;

        // Contatore dei cicli rimanenti per la sosta del veicolo
        let mut pause_counter = 0;

        loop {
            if pause_counter > 0 {
                // Veicolo fermo: decrementiamo il contatore e manteniamo la posizione invariata
                pause_counter -= 1;
            } else {
                let rng: f64 = rand::thread_rng().gen();

                if rng < PAUSE_PROBABILITY {
                    // Inizia una sosta
                    pause_counter = PAUSE_CYCLES;
                } else {
                    // Movimento casuale (random walk)
                    lat += rand::thread_rng().gen_range(-0.01..0.01);
                    lon += rand::thread_rng().gen_range(-0.01..0.01);
                }
            }

            // Costruiamo le nuove coordinate e il messaggio per il server
            let coords = Coordinates {
                latitude: lat,
                longitude: lon,
            };

            let msg = Message::PositionUpdate {
                user_id: user_id.clone(),
                coords,
                timestamp: Utc::now(),
            };

            // Inviamo il messaggio al canale interno del client.
            // Se il canale è chiuso (es. sessione terminata), interrompiamo il task.
            if tx_gps.send(msg).await.is_err() {
                break;
            }

            // Attesa periodica prima della successiva rilevazione GPS
            tokio::time::sleep(Duration::from_secs(UPDATE_INTERVAL_SECS)).await;
        }
    })
}
