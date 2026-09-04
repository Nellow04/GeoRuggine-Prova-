# Documentazione Tecnica: Gestione della Concorrenza, Lock e Await

Questo documento descrive dettagliatamente la riprogettazione della concorrenza in **GeoRuggine**, evidenziando gli anti-pattern che erano presenti nel codice originale, le soluzioni ingegneristiche adottate e i principi da esporre in sede d'esame.

---

## 1. I Tre Principi Guida del Refactoring

1. **Separazione delle Risorse Condivise (Separation of Concerns)**:
   - La mappa in memoria dei client connessi (`clients`) richiede mutua esclusione asincrona (`RwLock`).
   - Il connection pool del database (`db_pool`, basato su `r2d2`) è **già intrinsecamente thread-safe** e non necessita di alcun lock applicativo.
   - **Soluzione**: Rimuovere `db_pool` dall'interno di `RwLock<ServerState>`.

2. **Zero `.await` sotto Lock (Deadlock & Stall Prevention)**:
   - In Tokio, trattenere una guardia di lock (`RwLockReadGuard` o `RwLockWriteGuard`) attraverso un punto di sospensione asincrono (`.await` su socket TCP o su `mpsc::Sender`) è un anti-pattern grave: se il client remoto è lento o il buffer del canale è pieno, l'intero server si blocca in attesa del rilascio del lock.
   - **Soluzione**: Adottare il pattern a **Scope Ristretto `{ ... }`**: si acquisisce il lock, si estraggono/clonano i dati strettamente necessari e il lock viene distrutto dal compilatore prima di qualsiasi `.await`.

3. **Operazioni Bloccanti e Calcoli Pesanti Fuori dai Lock**:
   - L'hashing delle password (`Argon2`, CPU-bound) e le query su disco SQLite (`rusqlite`, I/O sincrono) non devono mai essere eseguiti all'interno del lock di memoria dei client.

---

## 2. Riepilogo Modifiche per File

| File | Prima (Anti-pattern) | Dopo (Standard Adottato) |
| :--- | :--- | :--- |
| [state.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/state.rs) | `Arc<RwLock<ServerState>>` inglobava sia `clients` sia `db_pool`. | `Arc<ServerState>` con `clients: RwLock<...>` e `db_pool: DbPool` libero. |
| [main.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/main.rs) | Inizializzazione con lock unico. | Inizializzazione disaccoppiata. |
| [cli.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/cli.rs) | `handle_broadcast` e `handle_private_message` facevano `.send().await` dentro il ReadLock. `/chat` prendeva il lock solo per accedere al DB. | Sender clonati in micro-scope `{ ... }` e invii eseguiti a lock rilasciato. Query DB completamente lock-free. |
| [tasks.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/tasks.rs) | `db::insert_state` eseguito dentro il ciclo `for` mentre si teneva il WriteLock. | WriteLock usato solo per aggiornare la memoria e raccogliere gli ID; scritture SQLite eseguite a lock rilasciato. |
| [connection.rs](file:///c:/Users/Lenovo%20I7/Documents/GeoRuggine-Prova-/server/src/connection.rs) | WriteLock trattenuto durante `write_all().await`, hashing Argon2 e query SQLite su registrazione, login, logout e GPS. | Registrazione e chat lock-free. Login, logout e GPS usano WriteLock ristretto a pochi microsecondi e nessun `.await` sotto lock. |

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
  pub type SharedState = Arc<RwLock<ServerState>>; // FIXME presente nel codice!
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

### B. Modulo Console Server: `server/src/cli.rs`

#### 1. Invio Broadcast (`/b`)
- **Prima**:
  ```rust
  let r_state = state.read().await;
  for (_, client) in r_state.clients.iter() {
      client.sender.send(broadcast_msg.clone()).await; // <-- TRATTENIMENTO LOCK!
  }
  ```
- **Dopo (Risolto)**:
  ```rust
  // Il ReadLock vive solo all'interno delle graffe per clonare i sender:
  let senders: Vec<_> = {
      let clients = state.clients.read().await;
      clients.values().map(|c| c.sender.clone()).collect()
  }; // <-- Lock distrutto qui!

  // Gli invii asincroni avvengono a lock rilasciato:
  for sender in senders {
      let _ = sender.send(broadcast_msg.clone()).await;
  }
  ```

#### 2. Storico Chat (`/chat`)
- **Prima**: Acquisiva `state.read().await` solo per invocare `db::get_user_by_name(&r_state.db_pool, ...)`.
- **Dopo (Risolto)**: Accesso diretto `db::get_user_by_name(&state.db_pool, ...)` con **zero lock**.

---

### C. Modulo Task Periodici: `server/src/tasks.rs`
- **Problema originario**: Il monitor di inattività iterava su `clients` con un WriteLock e, per ogni utente fermo, chiamava la funzione sincrona SQLite `db::insert_state(&pool, ...)` tenendo bloccati tutti gli altri task per l'intera durata dell'I/O su disco.
- **Soluzione applicata**:
  ```rust
  let users_to_persist: Vec<UserId> = {
      let mut clients = state.clients.write().await;
      // aggiornamento in memoria e raccolta ID
  }; // <-- WriteLock rilasciato!

  // Scrittura SQLite eseguita a lock rilasciato:
  for user_id in users_to_persist {
      let _ = db::insert_state(&state.db_pool, &user_id, "Fermo", now);
  }
  ```

---

### D. Modulo Connessioni: `server/src/connection.rs`

#### 1. Registrazione (`Message::RegisterRequest`)
- **Prima**: Acquisiva `state.write().await`, eseguiva l'hashing lento della password, la query di inserimento e infine `write_half.write_all().await` mentre teneva il WriteLock.
- **Dopo**: La registrazione non tocca la memoria dei client connessi: viene eseguita con **zero lock**, lasciando liberi tutti i thread di continuare a scambiarsi messaggi e posizioni GPS.

#### 2. Login (`Message::LoginRequest`)
- **Prima**: Tutto il blocco era coperto da un gigantesco WriteLock.
- **Dopo**:
  1. Controllo duplicati: brevissimo `state.clients.read().await`.
  2. Verifica hash Argon2 e query SQLite su `state.db_pool`: **nessun lock**.
  3. Registrazione del client: WriteLock limitato alla sola istruzione `clients.insert(...)`.
  4. Scrittura stato nel DB e invio risposta di rete TCP: **a lock rilasciato**.

#### 3. Aggiornamento Posizione GPS (`Message::PositionUpdate`)
- **Prima**: WriteLock mantenuto durante il calcolo e le chiamate di persistenza `db::insert_distance` e `db::insert_state`.
- **Dopo**: Si calcolano distanza e cambi di stato dentro un WriteLock minimale, si restituisce una tupla con i dati da persistere, il lock cade e la scrittura su DB avviene all'esterno.

#### 4. Chat Client-to-Server (`Message::ClientToServerText`)
- **Prima**: Prendeva un WriteLock esclusivo anche se leggeva solo il nome del mittente.
- **Dopo**: Breve ReadLock per leggere il nome; salvataggio su DB a lock rilasciato.

---

## 4. Domande Tipiche del Professore & Risposte Ragionate

> **D: Perché hai separato `db_pool` da `RwLock` in `ServerState`?**  
> **R:** Perché `r2d2::Pool` è già thread-safe ed è progettato per gestire autonomamente la concorrenza tra connessioni SQLite. Incapsularlo in un `RwLock` creava una serializzazione artificiale (collo di bottiglia) che costringeva le query su disco a bloccare la memoria volatile dei client.

> **D: Perché è rischioso fare `.await` mentre si detiene una guardia di lock?**  
> **R:** Perché un punto di `.await` restituisce il controllo all'executor di Tokio. Se il socket o il channel sono lenti a rispondere, il task rimane sospeso trattenendo il lock: qualsiasi altro task che tenti di acquisire il medesimo lock rimarrà a sua volta bloccato, portando a starvation del runtime o a deadlock incrociati.

> **D: Come garantisci che i client non ricevano messaggi corrotti durante il broadcast?**  
> **R:** Clonando gli handle dei canali (`mpsc::Sender<Message>`) all'interno di un breve blocco a lettura protetta. Il clone di un `Sender` è un'operazione atomica ed economica (incrementa un contatore `Arc` interno). Una volta ottenuti i sender, il lock viene rilasciato e ciascun invio asincrono è completamente isolato.
