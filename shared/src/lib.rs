use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub type UserId = String;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Coordinates {
    pub latitude: f64,
    pub longitude: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum UserState {
    Disconnesso,
    Fermo,
    InMovimento,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct User {
    pub id: UserId,
    pub username: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Message {
    /// Registrazione di un nuovo utente
    RegisterRequest { username: String, password: String },
    /// Risposta alla registrazione
    RegisterResponse { success: bool, message: String },
    /// Richiesta di login
    LoginRequest { username: String, password: String },
    /// Risposta al login
    LoginResponse { success: bool, user_id: Option<UserId>, message: String },
    /// Richiesta di logout
    LogoutRequest { user_id: UserId },
    /// Risposta al logout
    LogoutResponse { success: bool, message: String },
    /// Invio periodico delle coordinate
    PositionUpdate { user_id: UserId, coords: Coordinates, timestamp: DateTime<Utc> },
    /// Messaggio di testo dal client al server
    ClientToServerText { user_id: UserId, content: String },
    /// Messaggio di testo dal server a un client (Direct)
    ServerToClientDirect { target_user_id: UserId, content: String },
    /// Messaggio di testo dal server a tutti i client (Broadcast)
    ServerToClientBroadcast { content: String },
}
