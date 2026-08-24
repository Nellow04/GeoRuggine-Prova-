use chrono::{DateTime, Utc};
use shared::{Coordinates, UserState};

pub struct AnalysisResult {
    pub total_distance_km: f64,
    pub average_speed_kmh: f64,
    pub moving_time_secs: i64,
    pub pause_time_secs: i64,
}

/// Calcola la distanza in chilometri usando la formula di Haversine
fn haversine_distance(coord1: &Coordinates, coord2: &Coordinates) -> f64 {
    let r = 6371.0; // Raggio della terra in km
    let d_lat = (coord2.latitude - coord1.latitude).to_radians();
    let d_lon = (coord2.longitude - coord1.longitude).to_radians();

    let lat1 = coord1.latitude.to_radians();
    let lat2 = coord2.latitude.to_radians();

    let a = (d_lat / 2.0).sin().powi(2)
        + lat1.cos() * lat2.cos() * (d_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

    r * c
}

pub fn analyze_movement(
    history: &[(Coordinates, DateTime<Utc>)],
    _start_time: DateTime<Utc>,
    _end_time: DateTime<Utc>,
) -> AnalysisResult {
    // In questo prototipo base non filtriamo per orario (start_time, end_time) ma 
    // l'estensione iter().filter() verrebbe applicata qui.

    let mut total_distance_km = 0.0;
    let mut moving_time_secs = 0;
    let mut pause_time_secs = 0;

    if history.len() < 2 {
        return AnalysisResult {
            total_distance_km: 0.0,
            average_speed_kmh: 0.0,
            moving_time_secs: 0,
            pause_time_secs: 0,
        };
    }

    let mut current_state = UserState::Fermo;
    let mut last_move_time = history[0].1;

    for i in 1..history.len() {
        let (prev_coord, prev_time) = &history[i - 1];
        let (curr_coord, curr_time) = &history[i];

        let distance = haversine_distance(prev_coord, curr_coord);
        let time_diff = curr_time.signed_duration_since(*prev_time).num_seconds();

        // Se la posizione cambia, si è mosso
        if distance > 0.001 { // Soglia minima per considerare movimento
            total_distance_km += distance;
            
            if current_state == UserState::Fermo {
                // Eravamo fermi e ci stiamo muovendo
                current_state = UserState::InMovimento;
                pause_time_secs += prev_time.signed_duration_since(last_move_time).num_seconds();
            }
            moving_time_secs += time_diff;
            last_move_time = *curr_time;
        } else {
            // Posizione inalterata
            if current_state == UserState::InMovimento {
                let diff_from_last_move = curr_time.signed_duration_since(last_move_time).num_seconds();
                // Dopo 3 minuti (180 sec) passa a Fermo
                if diff_from_last_move >= 180 {
                    current_state = UserState::Fermo;
                    moving_time_secs += 180; // I primi 3 min erano considerati movimento, o pausa?
                    // Secondo i requisiti il passaggio a fermo si verifica se "per 3 min non cambia"
                    // Questo significa che da last_move_time è iniziata una pausa.
                    pause_time_secs += diff_from_last_move; 
                }
            } else {
                pause_time_secs += time_diff;
            }
        }
    }

    let average_speed_kmh = if moving_time_secs > 0 {
        total_distance_km / (moving_time_secs as f64 / 3600.0)
    } else {
        0.0
    };

    AnalysisResult {
        total_distance_km,
        average_speed_kmh,
        moving_time_secs,
        pause_time_secs,
    }
}
