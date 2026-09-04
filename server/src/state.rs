use chrono::{DateTime, Utc};
use shared::{Coordinates, Message, UserId, UserState};

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, RwLock};

use crate::db::DbPool;

// ============================================================
// DATI CLIENT CONNESSO
// ============================================================

#[derive(Debug, Clone)]
pub struct ClientData {
    pub username: String,
    pub state: UserState,
    pub last_position: Option<Coordinates>,
    pub last_move_time: Option<DateTime<Utc>>,
    pub state_history: Vec<(UserState, DateTime<Utc>)>,
    pub distance_history: Vec<(f64, DateTime<Utc>)>,
    pub sender: mpsc::Sender<Message>,
}

// ============================================================
// STATO GLOBALE SERVER
// ============================================================

/// Stato condiviso dell'applicazione.
///
/// Disaccoppia la gestione della memoria in-flight (`clients`) dal database (`db_pool`):
/// - `clients`: richiede sincronizzazione esplicita tramite `RwLock` per gestire le connessioni attive.
/// - `db_pool`: è già intrinsecamente thread-safe (`r2d2::Pool` si basa internamente su `Arc`),
///   quindi può essere acceduto liberamente e in parallelo senza alcun lock su `clients`.
pub struct ServerState {
    pub clients: RwLock<HashMap<UserId, ClientData>>,
    pub db_pool: DbPool,
}

// ============================================================
// STATO CONDIVISO
// ============================================================

pub type SharedState = Arc<ServerState>;