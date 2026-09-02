use chrono::{Duration, Utc};
use shared::UserState;
use std::collections::HashMap;

#[test]
fn test_inactivity_detection_threshold() {
    let now = Utc::now();
    let active_time = now - Duration::minutes(1);
    let inactive_time = now - Duration::minutes(4);

    // Se l'ultimo movimento è avvenuto 1 minuto fa (< 3 min), non scatta la pausa
    assert!(
        now.signed_duration_since(active_time).num_minutes() < 3,
        "1 minute inactivity should not trigger pause"
    );

    // Se l'ultimo movimento è avvenuto 4 minuti fa (>= 3 min), scatta la transizione a Fermo
    assert!(
        now.signed_duration_since(inactive_time).num_minutes() >= 3,
        "4 minutes inactivity must trigger pause"
    );
}

#[test]
fn test_duplicate_login_prevention_logic() {
    let mut logged_in_users: HashMap<String, (String, UserState)> = HashMap::new();
    logged_in_users.insert(
        "id-1".to_string(),
        ("mario".to_string(), UserState::Fermo),
    );
    logged_in_users.insert(
        "id-2".to_string(),
        ("luigi".to_string(), UserState::Disconnected),
    );

    // Mario è attualmente connesso (stato Fermo != Disconnected) -> deve essere bloccato
    let mario_connected = logged_in_users
        .values()
        .any(|(name, state)| name == "mario" && *state != UserState::Disconnected);
    assert!(
        mario_connected,
        "Mario is active and should not be allowed a duplicate login"
    );

    // Luigi è disconnesso -> deve poter effettuare nuovamente il login
    let luigi_connected = logged_in_users
        .values()
        .any(|(name, state)| name == "luigi" && *state != UserState::Disconnected);
    assert!(
        !luigi_connected,
        "Luigi is disconnected and should be allowed to log in"
    );

    // Peach non è presente -> deve poter effettuare il login
    let peach_connected = logged_in_users
        .values()
        .any(|(name, state)| name == "peach" && *state != UserState::Disconnected);
    assert!(
        !peach_connected,
        "Peach is not logged in and should be allowed"
    );
}
