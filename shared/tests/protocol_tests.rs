use chrono::Utc;
use shared::{Coordinates, Message, UserState};

#[test]
fn test_coordinates_serialization() {
    let coords = Coordinates {
        latitude: 45.0703,
        longitude: 7.6869,
    };
    let json = serde_json::to_string(&coords).expect("Serialization failed");
    let deserialized: Coordinates = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(coords, deserialized);
}

#[test]
fn test_user_state_serialization() {
    let states = vec![UserState::Disconnected, UserState::Fermo, UserState::InMovimento];
    for state in states {
        let json = serde_json::to_string(&state).expect("Serialization failed");
        let deserialized: UserState = serde_json::from_str(&json).expect("Deserialization failed");
        assert_eq!(state, deserialized);
    }
}

#[test]
fn test_message_registration_roundtrip() {
    let reg_req = Message::RegisterRequest {
        username: "mario".to_string(),
        password: "password123".to_string(),
    };
    let json = serde_json::to_string(&reg_req).expect("Serialization failed");
    let parsed: Message = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(reg_req, parsed);

    let reg_resp = Message::RegisterResponse {
        success: true,
        message: "Registrazione completata".to_string(),
    };
    let json = serde_json::to_string(&reg_resp).expect("Serialization failed");
    let parsed: Message = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(reg_resp, parsed);
}

#[test]
fn test_message_login_roundtrip() {
    let login_req = Message::LoginRequest {
        username: "luigi".to_string(),
        password: "secret".to_string(),
    };
    let json = serde_json::to_string(&login_req).expect("Serialization failed");
    let parsed: Message = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(login_req, parsed);

    let login_resp = Message::LoginResponse {
        success: true,
        user_id: Some("uuid-1234".to_string()),
        message: "Benvenuto luigi".to_string(),
    };
    let json = serde_json::to_string(&login_resp).expect("Serialization failed");
    let parsed: Message = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(login_resp, parsed);
}

#[test]
fn test_message_position_update_roundtrip() {
    let now = Utc::now();
    let pos_msg = Message::PositionUpdate {
        user_id: "user-1".to_string(),
        coords: Coordinates {
            latitude: 45.1,
            longitude: 7.2,
        },
        timestamp: now,
    };
    let json = serde_json::to_string(&pos_msg).expect("Serialization failed");
    let parsed: Message = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(pos_msg, parsed);
}

#[test]
fn test_chat_messages_roundtrip() {
    let text_msg = Message::ClientToServerText {
        user_id: "u1".to_string(),
        content: "Ciao Server".to_string(),
    };
    let json = serde_json::to_string(&text_msg).expect("Serialization failed");
    let parsed: Message = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(text_msg, parsed);

    let direct_msg = Message::ServerToClientDirect {
        target_user_id: "Server".to_string(),
        content: "Ciao Utente".to_string(),
    };
    let json = serde_json::to_string(&direct_msg).expect("Serialization failed");
    let parsed: Message = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(direct_msg, parsed);

    let bcast_msg = Message::ServerToClientBroadcast {
        content: "Avviso a tutti".to_string(),
    };
    let json = serde_json::to_string(&bcast_msg).expect("Serialization failed");
    let parsed: Message = serde_json::from_str(&json).expect("Deserialization failed");
    assert_eq!(bcast_msg, parsed);
}
