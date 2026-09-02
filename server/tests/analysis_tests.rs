use chrono::{Duration, Utc};
use server::analysis::{analyze_movement, calculate_distance};
use shared::{Coordinates, UserState};

#[test]
fn test_calculate_distance_zero() {
    let c = Coordinates {
        latitude: 45.0,
        longitude: 7.0,
    };
    let dist = calculate_distance(&c, &c);
    assert!(dist.abs() < 1e-6, "Distance to same coordinate must be 0");
}

#[test]
fn test_calculate_distance_known_cities() {
    // Roma: 41.9028, 12.4964; Milano: 45.4642, 9.1900 (~477 km in linea d'aria)
    let roma = Coordinates {
        latitude: 41.9028,
        longitude: 12.4964,
    };
    let milano = Coordinates {
        latitude: 45.4642,
        longitude: 9.1900,
    };
    let dist = calculate_distance(&roma, &milano);
    assert!(
        (dist - 477.0).abs() < 5.0,
        "Distance Rome-Milan should be ~477 km, got {}",
        dist
    );
}

#[test]
fn test_analyze_movement_empty_history() {
    let now = Utc::now();
    let result = analyze_movement(&[], &[], now - Duration::hours(1), now);
    assert_eq!(result.total_distance_km, 0.0);
    assert_eq!(result.average_speed_kmh, 0.0);
    assert_eq!(result.moving_time_secs, 0);
    assert_eq!(result.pause_time_secs, 0);
}

#[test]
fn test_analyze_movement_moving_and_pause() {
    let t0 = Utc::now() - Duration::minutes(20);
    let t1 = t0 + Duration::minutes(10); // 10 min movimento
    let t2 = t1 + Duration::minutes(5);  // 5 min fermo
    let t3 = t2 + Duration::minutes(5);  // 5 min disconnesso

    let state_history = vec![
        (UserState::InMovimento, t0),
        (UserState::Fermo, t1),
        (UserState::Disconnected, t2),
    ];

    let distance_history = vec![
        (5.0, t0 + Duration::minutes(3)),
        (5.0, t0 + Duration::minutes(7)),
    ];

    let result = analyze_movement(&state_history, &distance_history, t0, t3);

    // Distanza: 10 km
    assert_eq!(result.total_distance_km, 10.0);
    // Tempo movimento: 600 secondi (10 min)
    assert_eq!(result.moving_time_secs, 600);
    // Tempo pausa: 300 secondi (5 min)
    assert_eq!(result.pause_time_secs, 300);
    // Velocità media: 10 km in 600s (0.1667h) = 60 km/h
    assert!((result.average_speed_kmh - 60.0).abs() < 0.1);
}

#[test]
fn test_analyze_movement_time_window_filtering() {
    let t0 = Utc::now() - Duration::hours(2);
    let t1 = t0 + Duration::minutes(30);
    let t2 = t0 + Duration::hours(1);

    let state_history = vec![(UserState::InMovimento, t0)];

    let distance_history = vec![
        (10.0, t0),                         // fuori finestra (prima di t1)
        (15.0, t1 + Duration::minutes(10)), // dentro finestra
    ];

    let result = analyze_movement(&state_history, &distance_history, t1, t2);
    assert_eq!(result.total_distance_km, 15.0);
}
