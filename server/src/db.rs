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

pub fn register_user(pool: &DbPool, id: &str, username: &str, password_hash: &str) -> SqliteResult<bool> {
    let conn = pool.get().unwrap();
    let result = conn.execute(
        "INSERT INTO users (id, username, password_hash) VALUES (?1, ?2, ?3)",
        rusqlite::params![id, username, password_hash],
    );
    match result {
        Ok(_) => Ok(true),
        Err(rusqlite::Error::SqliteFailure(e, _)) if e.code == rusqlite::ErrorCode::ConstraintViolation => Ok(false),
        Err(e) => Err(e),
    }
}

pub fn get_user_by_name(pool: &DbPool, username: &str) -> SqliteResult<Option<(String, String)>> {
    let conn = pool.get().unwrap();
    let mut stmt = conn.prepare("SELECT id, password_hash FROM users WHERE username = ?1")?;
    let mut rows = stmt.query(rusqlite::params![username])?;
    if let Some(row) = rows.next()? {
        let id: String = row.get(0)?;
        let hash: String = row.get(1)?;
        Ok(Some((id, hash)))
    } else {
        Ok(None)
    }
}

pub fn insert_distance(pool: &DbPool, user_id: &str, distance: f64, timestamp: chrono::DateTime<chrono::Utc>) -> SqliteResult<()> {
    let conn = pool.get().unwrap();
    conn.execute(
        "INSERT INTO distances (user_id, distance, timestamp) VALUES (?1, ?2, ?3)",
        rusqlite::params![user_id, distance, timestamp.to_rfc3339()],
    )?;
    Ok(())
}

pub fn insert_state(pool: &DbPool, user_id: &str, state: &str, timestamp: chrono::DateTime<chrono::Utc>) -> SqliteResult<()> {
    let conn = pool.get().unwrap();
    conn.execute(
        "INSERT INTO states (user_id, state, timestamp) VALUES (?1, ?2, ?3)",
        rusqlite::params![user_id, state, timestamp.to_rfc3339()],
    )?;
    Ok(())
}

pub fn insert_chat(pool: &DbPool, sender_id: &str, receiver_id: Option<&str>, content: &str, timestamp: chrono::DateTime<chrono::Utc>) -> SqliteResult<()> {
    let conn = pool.get().unwrap();
    conn.execute(
        "INSERT INTO chats (sender_id, receiver_id, content, timestamp) VALUES (?1, ?2, ?3, ?4)",
        rusqlite::params![sender_id, receiver_id, content, timestamp.to_rfc3339()],
    )?;
    Ok(())
}

pub fn get_user_history(
    pool: &DbPool,
    user_id: &str,
    start: chrono::DateTime<chrono::Utc>,
    end: chrono::DateTime<chrono::Utc>
) -> SqliteResult<(Vec<(shared::UserState, chrono::DateTime<chrono::Utc>)>, Vec<(f64, chrono::DateTime<chrono::Utc>)>)> {
    let conn = pool.get().unwrap();
    
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
}

pub fn get_chat_history(pool: &DbPool, user_id: &str) -> SqliteResult<Vec<(String, String, chrono::DateTime<chrono::Utc>)>> {
    let conn = pool.get().unwrap();
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
}
