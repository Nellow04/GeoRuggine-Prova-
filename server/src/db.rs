use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::Result as SqliteResult;

pub type DbPool = Pool<SqliteConnectionManager>;

pub fn init_db() -> SqliteResult<DbPool> {
    let manager = SqliteConnectionManager::file("database.db");
    let pool = Pool::new(manager).expect("Failed to create connection pool");

    let conn = pool.get().expect("Failed to get connection from pool");

    conn.execute(
        "CREATE TABLE IF NOT EXISTS users (
            id TEXT PRIMARY KEY,
            username TEXT UNIQUE NOT NULL,
            password_hash TEXT NOT NULL
        )",
        (),
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS distances (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            distance REAL NOT NULL,
            timestamp DATETIME NOT NULL
        )",
        (),
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS states (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id TEXT NOT NULL,
            state TEXT NOT NULL,
            timestamp DATETIME NOT NULL
        )",
        (),
    )?;

    conn.execute(
        "CREATE TABLE IF NOT EXISTS chats (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sender_id TEXT NOT NULL,
            receiver_id TEXT,
            content TEXT NOT NULL,
            timestamp DATETIME NOT NULL
        )",
        (),
    )?;

    Ok(pool)
}

pub async fn register_user(pool: &DbPool, id: &str, username: &str, password_hash: &str) -> SqliteResult<bool> {
    let pool = pool.clone();
    let id = id.to_string();
    let username = username.to_string();
    let password_hash = password_hash.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = pool.get().expect("Failed to get connection from pool");
        let result = conn.execute(
            "INSERT INTO users (id, username, password_hash) VALUES (?1, ?2, ?3)",
            rusqlite::params![id, username, password_hash],
        );
        match result {
            Ok(_) => Ok(true),
            Err(rusqlite::Error::SqliteFailure(e, _)) if e.code == rusqlite::ErrorCode::ConstraintViolation => Ok(false),
            Err(e) => Err(e),
        }
    })
    .await
    .expect("spawn_blocking for register_user panicked")
}

pub async fn get_user_by_name(pool: &DbPool, username: &str) -> SqliteResult<Option<(String, String)>> {
    let pool = pool.clone();
    let username = username.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = pool.get().expect("Failed to get connection from pool");
        let mut stmt = conn.prepare("SELECT id, password_hash FROM users WHERE username = ?1")?;
        let mut rows = stmt.query(rusqlite::params![username])?;
        if let Some(row) = rows.next()? {
            let id: String = row.get(0)?;
            let hash: String = row.get(1)?;
            Ok(Some((id, hash)))
        } else {
            Ok(None)
        }
    })
    .await
    .expect("spawn_blocking for get_user_by_name panicked")
}

pub async fn insert_distance(pool: &DbPool, user_id: &str, distance: f64, timestamp: chrono::DateTime<chrono::Utc>) -> SqliteResult<()> {
    let pool = pool.clone();
    let user_id = user_id.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = pool.get().expect("Failed to get connection from pool");
        conn.execute(
            "INSERT INTO distances (user_id, distance, timestamp) VALUES (?1, ?2, ?3)",
            rusqlite::params![user_id, distance, timestamp.to_rfc3339()],
        )?;
        Ok(())
    })
    .await
    .expect("spawn_blocking for insert_distance panicked")
}

pub async fn insert_state(pool: &DbPool, user_id: &str, state: &str, timestamp: chrono::DateTime<chrono::Utc>) -> SqliteResult<()> {
    let pool = pool.clone();
    let user_id = user_id.to_string();
    let state = state.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = pool.get().expect("Failed to get connection from pool");
        conn.execute(
            "INSERT INTO states (user_id, state, timestamp) VALUES (?1, ?2, ?3)",
            rusqlite::params![user_id, state, timestamp.to_rfc3339()],
        )?;
        Ok(())
    })
    .await
    .expect("spawn_blocking for insert_state panicked")
}

pub async fn insert_chat(pool: &DbPool, sender_id: &str, receiver_id: Option<&str>, content: &str, timestamp: chrono::DateTime<chrono::Utc>) -> SqliteResult<()> {
    let pool = pool.clone();
    let sender_id = sender_id.to_string();
    let receiver_id = receiver_id.map(|s| s.to_string());
    let content = content.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = pool.get().expect("Failed to get connection from pool");
        conn.execute(
            "INSERT INTO chats (sender_id, receiver_id, content, timestamp) VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params![sender_id, receiver_id, content, timestamp.to_rfc3339()],
        )?;
        Ok(())
    })
    .await
    .expect("spawn_blocking for insert_chat panicked")
}

pub async fn get_user_history(
    pool: &DbPool,
    user_id: &str,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>
) -> SqliteResult<(Vec<(shared::UserState, chrono::DateTime<chrono::Utc>)>, Vec<(f64, chrono::DateTime<chrono::Utc>)>)> {
    let pool = pool.clone();
    let user_id = user_id.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = pool.get().expect("Failed to get connection from pool");
        
        let mut states = Vec::new();
        let mut stmt = conn.prepare("SELECT state, timestamp FROM states WHERE user_id = ?1 AND timestamp >= ?2 AND timestamp <= ?3 ORDER BY timestamp ASC")?;
        let mut rows = stmt.query(rusqlite::params![user_id, start.to_rfc3339(), end.to_rfc3339()])?;
        while let Some(row) = rows.next()? {
            let state_str: String = row.get(0)?;
            let ts_str: String = row.get(1)?;
            let ts = chrono::DateTime::parse_from_rfc3339(&ts_str).unwrap().with_timezone(&chrono::Utc);
            let state = match state_str.as_str() {
                "In Movimento" => shared::UserState::InMovimento,
                "Disconnesso" | "Sconnesso" | "Disconnected" => shared::UserState::Disconnesso,
                _ => shared::UserState::Fermo,
            };
            states.push((state, ts));
        }

        // Se non abbiamo uno stato registrato a o prima di `start` (oppure `states` è vuoto),
        // recuperiamo l'ultimo stato registrato prima di `start` per conoscere lo stato dell'utente
        // all'inizio dell'intervallo temporale considerato.
        let needs_prior_state = states.first().map(|(_, ts)| *ts > start).unwrap_or(true);
        if needs_prior_state {
            let mut prior_stmt = conn.prepare(
                "SELECT state, timestamp FROM states WHERE user_id = ?1 AND timestamp < ?2 ORDER BY timestamp DESC LIMIT 1"
            )?;
            let mut prior_rows = prior_stmt.query(rusqlite::params![user_id, start.to_rfc3339()])?;
            if let Some(row) = prior_rows.next()? {
                let state_str: String = row.get(0)?;
                let ts_str: String = row.get(1)?;
                let ts = chrono::DateTime::parse_from_rfc3339(&ts_str).unwrap().with_timezone(&chrono::Utc);
                let state = match state_str.as_str() {
                    "In Movimento" => shared::UserState::InMovimento,
                    "Disconnesso" | "Sconnesso" | "Disconnected" => shared::UserState::Disconnesso,
                    _ => shared::UserState::Fermo,
                };
                states.insert(0, (state, ts));
            }
        }

        let mut distances = Vec::new();
        let mut stmt2 = conn.prepare("SELECT distance, timestamp FROM distances WHERE user_id = ?1 AND timestamp >= ?2 AND timestamp <= ?3 ORDER BY timestamp ASC")?;
        let mut rows2 = stmt2.query(rusqlite::params![user_id, start.to_rfc3339(), end.to_rfc3339()])?;
        while let Some(row) = rows2.next()? {
            let dist: f64 = row.get(0)?;
            let ts_str: String = row.get(1)?;
            let ts = chrono::DateTime::parse_from_rfc3339(&ts_str).unwrap().with_timezone(&chrono::Utc);
            distances.push((dist, ts));
        }

        Ok((states, distances))
    })
    .await
    .expect("spawn_blocking for get_user_history panicked")
}

pub async fn get_chat_history(pool: &DbPool, user_id: &str) -> SqliteResult<Vec<(String, String, chrono::DateTime<chrono::Utc>)>> {
    let pool = pool.clone();
    let user_id = user_id.to_string();

    tokio::task::spawn_blocking(move || {
        let conn = pool.get().expect("Failed to get connection from pool");
        let mut chats = Vec::new();
        // Prende i messaggi dove l'utente è mittente (e il ricevente è Server) o dove l'utente è ricevente (e il mittente è Server).
        let mut stmt = conn.prepare(
            "SELECT sender_id, content, timestamp FROM chats 
             WHERE (sender_id = ?1 AND receiver_id = 'Server') 
                OR (sender_id = 'Server' AND receiver_id = ?1) 
             ORDER BY timestamp ASC"
        )?;
        
        let mut rows = stmt.query(rusqlite::params![user_id])?;
        while let Some(row) = rows.next()? {
            let sender: String = row.get(0)?;
            let content: String = row.get(1)?;
            let ts_str: String = row.get(2)?;
            let ts = chrono::DateTime::parse_from_rfc3339(&ts_str).unwrap().with_timezone(&chrono::Utc);
            chats.push((sender, content, ts));
        }
        
        Ok(chats)
    })
    .await
    .expect("spawn_blocking for get_chat_history panicked")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_test_pool() -> DbPool {
        let manager = SqliteConnectionManager::memory();
        let pool = Pool::builder()
            .max_size(1)
            .build(manager)
            .expect("Failed to create in-memory pool");

        let conn = pool.get().expect("Failed to get connection");
        conn.execute(
            "CREATE TABLE IF NOT EXISTS users (
                id TEXT PRIMARY KEY,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL
            )",
            (),
        ).unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS states (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                state TEXT NOT NULL,
                timestamp DATETIME NOT NULL
            )",
            (),
        ).unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS distances (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id TEXT NOT NULL,
                distance REAL NOT NULL,
                timestamp DATETIME NOT NULL
            )",
            (),
        ).unwrap();
        conn.execute(
            "CREATE TABLE IF NOT EXISTS chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                sender_id TEXT NOT NULL,
                receiver_id TEXT,
                content TEXT NOT NULL,
                timestamp DATETIME NOT NULL
            )",
            (),
        ).unwrap();

        pool
    }

    #[tokio::test]
    async fn test_async_register_and_get_user() {
        let pool = init_test_pool();

        let ok = register_user(&pool, "u1", "testuser", "hashedpass").await.unwrap();
        assert!(ok);

        let duplicate = register_user(&pool, "u2", "testuser", "hashedpass2").await.unwrap();
        assert!(!duplicate);

        let fetched = get_user_by_name(&pool, "testuser").await.unwrap();
        assert_eq!(fetched, Some(("u1".to_string(), "hashedpass".to_string())));

        let not_found = get_user_by_name(&pool, "nonexistent").await.unwrap();
        assert_eq!(not_found, None);
    }

    #[tokio::test]
    async fn test_async_insert_and_get_history() {
        let pool = init_test_pool();
        let now = chrono::Utc::now();

        insert_state(&pool, "u1", "In Movimento", now).await.unwrap();
        insert_distance(&pool, "u1", 1.5, now).await.unwrap();

        let start = now - chrono::Duration::minutes(5);
        let end = now + chrono::Duration::minutes(5);

        let (states, distances) = get_user_history(&pool, "u1", start, end).await.unwrap();
        assert_eq!(states.len(), 1);
        assert_eq!(distances.len(), 1);
        assert_eq!(distances[0].0, 1.5);
    }

    #[tokio::test]
    async fn test_async_chat() {
        let pool = init_test_pool();
        let now = chrono::Utc::now();

        insert_chat(&pool, "u1", Some("Server"), "Ciao Server!", now).await.unwrap();
        insert_chat(&pool, "Server", Some("u1"), "Ciao Utente!", now + chrono::Duration::seconds(1)).await.unwrap();

        let history = get_chat_history(&pool, "u1").await.unwrap();
        assert_eq!(history.len(), 2);
        assert_eq!(history[0].1, "Ciao Server!");
        assert_eq!(history[1].1, "Ciao Utente!");
    }
}
