use chrono::{DateTime, Utc};
use shared::Coordinates;

pub struct AnalysisResult {
    pub total_distance_km: f64,
    pub average_speed_kmh: f64,
    pub moving_time_secs: i64,
    pub pause_time_secs: i64,
}

/// Calcola la distanza in chilometri usando la formula di Haversine
pub fn haversine_distance(coord1: &Coordinates, coord2: &Coordinates) -> f64 {
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
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> AnalysisResult {
    let filtered_history: Vec<&(Coordinates, DateTime<Utc>)> = history
        .iter()
        .filter(|(_, time)| *time >= start_time && *time <= end_time)
        .collect();

    let mut total_distance_km = 0.0;
    let mut moving_time_secs = 0;
    let mut pause_time_secs = 0;

    if filtered_history.len() < 2 {
        return AnalysisResult {
            total_distance_km: 0.0,
            average_speed_kmh: 0.0,
            moving_time_secs: 0,
            pause_time_secs: 0,
        };
    }



    for i in 1..filtered_history.len() {
        let (prev_coord, prev_time) = filtered_history[i - 1];
        let (curr_coord, curr_time) = filtered_history[i];

        let distance = haversine_distance(prev_coord, curr_coord);
        let time_diff = curr_time.signed_duration_since(*prev_time).num_seconds();

        // Se la posizione cambia (più di 1 metro)
        if distance > 0.001 {
            total_distance_km += distance;
            moving_time_secs += time_diff;
        } else {
            // La posizione NON cambia
            // Se la differenza di tempo è ragionevole (circa 30s del ping GPS, usiamo <= 45s di tolleranza)
            // significa che l'utente è connesso ma fermo. È una pausa!
            // Se invece time_diff > 45s, c'è stato un buco di connessione (Disconnesso), quindi NON è una pausa.
            if time_diff <= 45 {
                pause_time_secs += time_diff;
            }
        }
    }
    
    // Controlliamo il gap finale tra l'ultima posizione e end_time
    // Se l'utente è rimasto Fermo ma connesso, l'ultimo punto non include il tempo fino a "ora"
    // Questo lo lasciamo al vivo se fosse necessario, ma poiché il ping avviene ogni 30s, 
    // l'ultimo punto è sempre molto recente (max 30 sec fa) se l'utente è connesso.

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
