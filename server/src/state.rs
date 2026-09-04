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

pub struct ServerState {
    pub clients: HashMap<UserId, ClientData>,
    pub db_pool: DbPool,
}


// ============================================================
// STATO CONDIVISO
// ============================================================

// FIXME: rivedere gestione lock
pub type SharedState = Arc<RwLock<ServerState>>;