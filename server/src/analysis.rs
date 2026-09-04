use chrono::{DateTime, Utc};
use shared::{Coordinates, UserState};

pub struct AnalysisResult {
    pub total_distance_km: f64,
    pub average_speed_kmh: f64,
    pub moving_time_secs: i64,
    pub pause_time_secs: i64,
}

/// Calcola la distanza in chilometri usando la formula di Haversine
/// TODO: rivedi!
pub fn calculate_distance(coord1: &Coordinates, coord2: &Coordinates) -> f64 {
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
    state_history: &[(UserState, DateTime<Utc>)],
    distance_history: &[(f64, DateTime<Utc>)],
    start_time: DateTime<Utc>,
    end_time: DateTime<Utc>,
) -> AnalysisResult {
    // 1. Calcola Distanza Totale
    let mut total_distance_km = 0.0;
    for &(dist, time) in distance_history {
        if time >= start_time && time <= end_time {
            total_distance_km += dist;
        }
    }

    // 2. Calcola Tempi (Movimento vs Pausa)
    let mut moving_time_secs = 0;
    let mut pause_time_secs = 0;

    if state_history.is_empty() {
        return AnalysisResult {
            total_distance_km,
            average_speed_kmh: 0.0,
            moving_time_secs,
            pause_time_secs,
        };
    }

    let first_event_time = state_history[0].1;
    let effective_start = std::cmp::max(start_time, first_event_time);

    let mut current_eval_state = UserState::Disconnesso;
    let mut last_eval_time = effective_start;

    // Troviamo lo stato in cui l'utente si trovava a effective_start
    for &(ref state, time) in state_history {
        if time <= effective_start {
            current_eval_state = state.clone();
        }
    }

    // Valutiamo i cambi di stato successivi a effective_start, ma entro end_time
    for &(ref state, time) in state_history {
        if time > effective_start && time <= end_time {
            let duration = time.signed_duration_since(last_eval_time).num_seconds();
            match current_eval_state {
                UserState::InMovimento => moving_time_secs += duration,
                UserState::Fermo => pause_time_secs += duration,
                UserState::Disconnesso => {} // Il tempo disconnesso viene ignorato
            }
            current_eval_state = state.clone();
            last_eval_time = time;
        }
    }

    // Aggiungiamo l'ultimo spezzone di tempo, da last_eval_time fino a end_time (o fino ad "ora" se end_time è nel futuro)
    let end_limit = std::cmp::min(end_time, Utc::now());
    if end_limit > last_eval_time {
        let final_duration = end_limit.signed_duration_since(last_eval_time).num_seconds();
        match current_eval_state {
            UserState::InMovimento => moving_time_secs += final_duration,
            UserState::Fermo => pause_time_secs += final_duration,
            UserState::Disconnesso => {} 
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

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration, TimeZone, Utc};

    #[test]
    fn test_analyze_movement_with_prior_state() {
        // Intervallo: oggi dalle 00:00:00 alle 02:00:00
        let start = Utc.with_ymd_and_hms(2026, 9, 4, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2026, 9, 4, 2, 0, 0).unwrap();

        // L'utente era Fermo da ieri alle 23:00
        // Alle 01:00 inizia a muoversi
        // Alle 02:00 si ferma
        let states = vec![
            (UserState::Fermo, start - Duration::hours(1)), // ieri 23:00
            (UserState::InMovimento, start + Duration::hours(1)), // oggi 01:00
            (UserState::Fermo, start + Duration::hours(2)), // oggi 02:00
        ];

        let distances = vec![
            (10.0, start + Duration::hours(1) + Duration::minutes(30)), // 10 km percorsi
        ];

        let result = analyze_movement(&states, &distances, start, end);

        // Dalle 00:00 alle 01:00 (1 ora = 3600 sec) era Fermo
        assert_eq!(result.pause_time_secs, 3600);
        // Dalle 01:00 alle 02:00 (1 ora = 3600 sec) era InMovimento
        assert_eq!(result.moving_time_secs, 3600);
        assert_eq!(result.total_distance_km, 10.0);
        assert!((result.average_speed_kmh - 10.0).abs() < 0.01);
    }
}
