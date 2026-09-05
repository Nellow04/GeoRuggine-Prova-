# Documentazione Tecnica: Gestione della Concorrenza, Lock e Offloading Asincrono

Questo documento descrive dettagliatamente la riprogettazione della concorrenza e dell'asincronia in **GeoRuggine**, evidenziando gli anti-pattern che erano presenti nel codice originale, le soluzioni ingegneristiche adottate e i principi da esporre con autorevolezza in sede d'esame.

---

## 1. I Quattro Principi Guida dell'Architettura Concorrente

1. **Separazione delle Risorse Condivise (Separation of Concerns)**:
   - La mappa in memoria dei client connessi (`clients`) richiede mutua esclusione asincrona (`RwLock`).
   - Il connection pool del database (`db_pool`, basato su `r2d2`) è **già intrinsecamente thread-safe** e non necessita di alcun lock applicativo.
   - **Soluzione**: Rimuovere `db_pool` dall'interno di `RwLock<ServerState>` per evitare che query DB blocchino la memoria dei client.

2. **Zero `.await` sotto Lock (Deadlock & Stall Prevention)**:
   - In Tokio, trattenere una guardia di lock (`RwLockReadGuard` o `RwLockWriteGuard`) attraverso un punto di sospensione asincrono (`.await` su socket TCP o su `mpsc::Sender`) è un anti-pattern grave: se il client remoto è lento o il buffer del canale è pieno, l'intero server si blocca in attesa del rilascio del lock.
   - **Soluzione**: Adottare il pattern a **Scope Ristretto `{ ... }`**: si acquisisce il lock, si estraggono/clonano i dati strettamente necessari e il lock viene distrutto dal compilatore prima di qualsiasi `.await`.

3. **Operazioni Bloccanti e Calcoli Pesanti Fuori dai Lock**:
   - L'hashing delle password (`Argon2`, CPU-bound) e le query su disco SQLite (`rusqlite`, I/O sincrono) non devono mai essere eseguiti all'interno del lock di memoria dei client.

4. **Offloading delle Operazioni Sincrone dall'Executor Tokio (`tokio::task::spawn_blocking`)**:
   - Togliere le operazioni bloccanti dai lock **non basta**: il runtime Tokio si basa su **multitasking cooperativo**. Se una funzione asincrona esegue I/O sincrono (`rusqlite`, lettura file) o calcoli intensivi sulla CPU (`Argon2`), **monopolizza il thread worker del runtime**.
   - Poiché i worker thread di Tokio sono in numero limitato (pari ai core della CPU), bloccarli provoca la **starvation** (fame) di tutti gli altri task asincroni concorrenti (pings, connessioni TCP, ricezione GPS).
   - **Soluzione**: Incapsulare tutte le chiamate `rusqlite` e `Argon2` all'interno di `tokio::task::spawn_blocking`, delegandole al thread pool dedicato di Tokio per compiti bloccanti.

---

## 2. Riepilogo Modifiche per File

| File | Prima (Anti-pattern) | Dopo (Standard Adottato) |
| :--- | :--- | :--- |
| [state.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/state.rs) | `Arc<RwLock<ServerState>>` inglobava sia `clients` sia `db_pool`. | `Arc<ServerState>` con `clients: RwLock<...>` e `db_pool: DbPool` thread-safe libero. |
| [auth.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/auth.rs) | Hashing e verifica password con Argon2 eseguiti direttamente sul thread worker di Tokio (CPU stall). | Funzioni asincrone `hash_password` e `verify_password` con calcolo offloaded su `tokio::task::spawn_blocking`. |
| [db.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/db.rs) | Funzioni sincrone che chiamavano `pool.get()` e `conn.execute()` bloccando il worker thread del chiamante. | Funzioni `pub async fn` che incapsulano `r2d2` e `rusqlite` in `tokio::task::spawn_blocking` isolando l'I/O bloccante. |
| [tasks.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/tasks.rs) | `db::insert_state` e scrittura su file `OpenOptions` bloccanti e sotto WriteLock. | WriteLock usato solo per la memoria; scritture DB e log CPU delegate in modo asincrono non bloccante (`tokio::fs`). |
| [cli.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/cli.rs) | Invii dentro ReadLock e query DB sincrone. | Sender clonati in micro-scope `{ ... }`, `.await` di rete e query DB asincrone non bloccanti. |
| [connection.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/connection.rs) | WriteLock trattenuto durante `write_all().await`, hashing Argon2 e query SQLite su registrazione, login, logout e GPS. | Registrazione e chat lock-free. Double-checked locking su login. Tutte le query DB e operazioni Argon2 eseguite con `.await` non bloccante. |

---

## 3. Dettaglio Tecnico delle Correzioni

### A. Modulo Stato: `server/src/state.rs` e `server/src/main.rs`
- **Problema originario**:
  ```rust
  // VECCHIO:
  pub struct ServerState {
      pub clients: HashMap<UserId, ClientData>,
      pub db_pool: DbPool,
  }
  pub type SharedState = Arc<RwLock<ServerState>>; // Anti-pattern: pool dentro RwLock
  ```
  Qualsiasi operazione al database richiedeva `state.read().await` o `state.write().await`, costringendo thread indipendenti a mettersi in coda anche solo per leggere uno storico messaggi.
- **Codice Corretto**:
  ```rust
  // NUOVO:
  pub struct ServerState {
      pub clients: RwLock<HashMap<UserId, ClientData>>,
      pub db_pool: DbPool,
  }
  pub type SharedState = Arc<ServerState>;
  ```
  In questo modo `state.db_pool` è accessibile immediatamente e concorrentemente senza lock, mentre `state.clients` protegge esclusivamente le connessioni in memoria.

---

### B. Modulo Autenticazione e Calcolo CPU: `server/src/auth.rs`
- **Problema originario**: `Argon2` è volutamente calibrato per richiedere decine o centinaia di millisecondi di calcolo CPU al fine di contrastare attacchi brute-force. Eseguito all'interno di un task asincrono, congela il thread worker di Tokio.
- **Codice Corretto**:
  ```rust
  pub async fn hash_password(password: &str) -> Result<String, argon2::password_hash::Error> {
      let password = password.to_string();
      tokio::task::spawn_blocking(move || {
          let salt = SaltString::generate(&mut OsRng);
          let argon2 = Argon2::default();
          let password_hash = argon2.hash_password(password.as_bytes(), &salt)?.to_string();
          Ok(password_hash)
      })
      .await
      .expect("spawn_blocking panicked")
  }
  ```
  Il task asincrono cede il controllo (`.await`) mentre il calcolo viene svolto in parallelo su un thread del pool bloccante.

---

### C. Modulo Database: `server/src/db.rs`
- **Problema originario**: `rusqlite` e `r2d2` sono librerie sincrone. `pool.get()` acquisisce un mutex OS sincrono per estrarre una connessione, mentre `conn.execute()` o `stmt.query()` attendono l'I/O del disco.
- **Codice Corretto**:
  ```rust
  pub async fn get_user_by_name(pool: &DbPool, username: &str) -> SqliteResult<Option<(String, String)>> {
      let pool = pool.clone(); // Arc clone economico del pool
      let username = username.to_string();
      tokio::task::spawn_blocking(move || {
          let conn = pool.get().expect("Failed to get connection from pool");
          let mut stmt = conn.prepare("SELECT id, password_hash FROM users WHERE username = ?1")?;
          let mut rows = stmt.query(rusqlite::params![username])?;
          if let Some(row) = rows.next()? {
              Ok(Some((row.get(0)?, row.get(1)?)))
          } else {
              Ok(None)
          }
      })
      .await
      .expect("spawn_blocking panicked")
  }
  ```
  L'API esposta all'applicazione diventa pienamente asincrona (`async fn`), permettendo a Tokio di continuare a elaborare altri client durante l'I/O su database.

---

### D. Modulo Connessioni: `server/src/connection.rs`
- **Registrazione (`Message::RegisterRequest`)**:
  - Calcola l'hash con `auth::hash_password(&password).await` (in background).
  - Inserisce l'utente con `db::register_user(&state.db_pool, ...).await` (in background).
  - **Zero lock** su `clients`: l'operazione non interferisce con gli altri utenti attivi.
- **Login (`Message::LoginRequest`) con Double-Checked Locking**:
  1. *Filtro rapido*: breve `state.clients.read().await` per escludere sessioni duplicate.
  2. *Query e Calcolo CPU*: `db::get_user_by_name(&state.db_pool, ...).await` e `auth::verify_password(&password, &hash).await` eseguiti in background senza trattenere lock.
  3. *Inserimento atomico*: breve `state.clients.write().await` per inserire la sessione verificando che nel frattempo non sia subentrato un conflitto atomico.
  4. *Persistenza e Risposta*: `db::insert_state(...).await` e invio su socket di rete TCP eseguiti a lock rilasciato.
- **GPS, Chat e Logout**:
  - Tutta la memoria volatile viene aggiornata in scope protetti microscopici.
  - Tutte le scritture `insert_distance`, `insert_state`, `insert_chat` avvengono a lock rilasciato con chiamate asincrone `.await`.

---

## 4. Domande Tipiche del Professore & Risposte Ragionate

> **D: Perché hai separato `db_pool` da `RwLock` in `ServerState`?**  
> **R:** Perché `r2d2::Pool` è già thread-safe (utilizza internamente puntatori atomici e mutex granulari) ed è progettato per gestire autonomamente la concorrenza tra connessioni SQLite. Incapsularlo in un `RwLock` applicativo creava una serializzazione artificiale (collo di bottiglia) che costringeva le query su disco a bloccare l'accesso alla memoria volatile di tutti i client connessi.

> **D: Perché non basta aver tolto `db_pool` e `Argon2` dal lock dei client? Perché serve anche `spawn_blocking`?**  
> **R:** Rimuovere le operazioni dal lock risolve il problema della **contesa dei lock (Lock Contention)**, ma non quello della **fame dei thread worker (Worker Starvation)**. Il runtime Tokio adotta uno scheduling cooperativo: i worker thread (tipicamente 1 per core) devono eseguire solo task non bloccanti. Se un task esegue calcoli CPU intensivi (`Argon2`) o I/O sincrono bloccante (`rusqlite`), blocca il thread worker stesso. Di conseguenza, tutti gli altri task assegnati a quel worker thread rimangono congelati. Con `spawn_blocking`, Tokio sposta il lavoro su un threadpool ausiliario dedicato ai task bloccanti, liberando immediatamente il worker thread.

> **D: Perché è rischioso fare `.await` mentre si detiene una guardia di lock?**  
> **R:** Perché un punto di `.await` restituisce il controllo all'executor di Tokio. Se il socket o il channel sono lenti a rispondere, il task rimane sospeso trattenendo il lock: qualsiasi altro task che tenti di acquisire il medesimo lock rimarrà a sua volta bloccato, portando a starvation del runtime o a deadlock incrociati.

> **D: Come si differenzia `tokio::spawn` da `tokio::task::spawn_blocking`?**  
> **R:** `tokio::spawn` avvia una `Future` asincrona sui worker thread del runtime asincrono principale (pool a work-stealing non bloccante). Al contrario, `tokio::task::spawn_blocking` riceve una closure sincrona standard e la invia a un pool separato di thread nativi del sistema operativo, configurato per scalare fino a 512 thread, pensato specificamente per assorbire chiamate di sistema bloccanti, I/O sincrono o calcoli CPU-heavy senza intaccare la reattività dell'event loop asincrono.

> **D: Come garantisci che i client non ricevano messaggi corrotti durante il broadcast?**  
> **R:** Clonando gli handle dei canali (`mpsc::Sender<Message>`) all'interno di un breve blocco a lettura protetta. Il clone di un `Sender` è un'operazione atomica ed economica (incrementa un contatore `Arc` interno). Una volta ottenuti i sender, il lock viene rilasciato e ciascun invio asincrono è completamente isolato.

---

## 5. Suite di Test Automatizzati: Cosa Testiamo e Come Funzionano

La suite di test del progetto è composta da **10 test automatici** distribuiti nei moduli di competenza tramite la direttiva standard di Rust `#[cfg(test)]`. I test vengono eseguiti con:

```bash
cargo test
```

### Tabella Riassuntiva dei Test

| Modulo | Nome del Test | Tipo | Obiettivo e Proprietà Verificate |
| :--- | :--- | :--- | :--- |
| **`auth.rs`** | `test_async_hash_and_verify_success` | Asincrono (`#[tokio::test]`) | Generazione corretta del salt casuale e dell'hash PHC con Argon2 su threadpool separato; verifica positiva della corrispondenza password in chiaro / hash. |
| **`auth.rs`** | `test_async_verify_wrong_password` | Asincrono (`#[tokio::test]`) | Rifiuto categorico (`false`) se viene passata una password errata contro l'hash valido salvato. |
| **`db.rs`** | `test_async_register_and_get_user` | Asincrono (`#[tokio::test]`) | Inserimento asincrono dell'utente; gestione corretta del vincolo di unicità `UNIQUE` su username (rifiuto secondo utente omonimo); recupero per nome con deserializzazione campi (`id`, `hash`). |
| **`db.rs`** | `test_async_insert_and_get_history` | Asincrono (`#[tokio::test]`) | Persistenza non bloccante di stati utente e distanze GPS; recupero dello storico filtrato per intervallo temporale `[start, end]`. |
| **`db.rs`** | `test_async_chat` | Asincrono (`#[tokio::test]`) | Inserimento concorrente e recupero cronologico ordinato (`ORDER BY timestamp ASC`) dei messaggi di chat scambiati bidirezionalmente tra utente e `Server`. |
| **`analysis.rs`** | `test_analyze_movement_with_prior_state` | Sincrono (`#[test]`) | Algoritmo di analisi temporale: corretto calcolo del tempo di pausa (3600s) e di movimento (3600s), distanza totale (10 km) e velocità media (10 km/h) quando lo stato iniziale dell'utente risale all'intervallo precedente (stato pregresso). |
| **`cli.rs`** | `test_calculate_interval_bounds_giorno` | Sincrono (`#[test]`) | Parsing finestra `/stats ... giorno`: limite inferiore impostato rigorosamente alla mezzanotte `00:00:00 UTC` del giorno corrente. |
| **`cli.rs`** | `test_calculate_interval_bounds_settimana` | Sincrono (`#[test]`) | Parsing finestra `/stats ... settimana`: calcolo accurato del Lunedì precedente alle `00:00:00 UTC`, verificando anche casi limite (venerdì, domenica sera, lunedì mattina). |
| **`cli.rs`** | `test_calculate_interval_bounds_mese` | Sincrono (`#[test]`) | Parsing finestra `/stats ... mese`: limite inferiore impostato al primo giorno del mese corrente alle `00:00:00 UTC`. |
| **`cli.rs`** | `test_calculate_interval_bounds_all` | Sincrono (`#[test]`) | Parsing finestra `/stats ... all`: copertura completa dell'intero storico a partire da `DateTime::<Utc>::MIN_UTC`. |

---

### Dettaglio Tecnico delle Tecnologie di Test Adottate

1. **Test Asincroni con Runtime Dedicato (`#[tokio::test]`)**:
   - I test di `auth.rs` e `db.rs` utilizzano la macro `#[tokio::test]`, che avvia un mini-runtime Tokio isolato per ciascun test, permettendo di eseguire chiamate `.await` e validare il corretto funzionamento di `tokio::task::spawn_blocking`.
2. **Database SQLite In-Memory Isolato**:
   - Per testare `db.rs` senza sporcare o dipendere dal file `database.db` di produzione, viene inizializzato un pool SQLite `r2d2` in-memory (`SqliteConnectionManager::memory()`) con dimensione massima 1 (`max_size(1)`). Questo garantisce che la tabella in memoria persista per la durata dell'intero test e venga distrutta alla fine.
3. **Determinismo Temporale**:
   - I test di `analysis.rs` e `cli.rs` utilizzano timestamp UTC espliciti e controllati (`Utc.with_ymd_and_hms(...)`), eliminando qualsiasi dipendenza dal fuso orario locale o dall'orario di sistema della macchina che esegue i test.


