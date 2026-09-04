use std::error::Error;
use std::io::{self, Write};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use shared::Message;

/// Tipo restituito al termine di un'autenticazione avvenuta con successo:
/// `(user_id, write_half, reader)`
pub type AuthResult = (
    String,
    OwnedWriteHalf,
    BufReader<OwnedReadHalf>,
);

/// Scelte disponibili nel menu iniziale
enum AuthChoice {
    Login,
    Register,
}

/// Gestisce l'intero flusso di autenticazione iniziale (interfaccia utente e handshake TCP).
///
/// Cicla finché l'utente non completa un login con esito positivo:
/// 1. Mostra il menu e acquisisce scelta (Login o Registrazione).
/// 2. Chiede credenziali (username e password).
/// 3. Apre la connessione TCP con il server ("127.0.0.1:8080").
/// 4. Se registrazione: invia `RegisterRequest` e, se ha successo, procede al login automatico.
/// 5. Invia `LoginRequest` e restituisce lo `user_id` insieme ai due stream della connessione.
pub async fn authenticate() -> Result<AuthResult, Box<dyn Error>> {
    loop {
        println!("=== GeoRuggine ===");
        println!("1) Login");
        println!("2) Registrazione");

        let choice = read_choice()?;
        let (username, password) = read_credentials()?;

        // Connessione al server TCP
        let stream = match TcpStream::connect("127.0.0.1:8080").await {
            Ok(stream) => stream,
            Err(e) => {
                eprintln!("Impossibile connettersi al server: {}. Riprova.\n", e);
                continue;
            }
        };

        println!("Connesso al server.");

        // Separazione degli stream per lettura e scrittura concorrenti
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        if let AuthChoice::Register = choice {
            let registration_success = register(
                &username,
                &password,
                &mut write_half,
                &mut reader,
            ).await?;

            if !registration_success {
                println!("Riprova.\n");
                continue;
            }

            println!("Login automatico...\n");
        }

        // Il login viene effettuato sia per la scelta 1 che dopo la registrazione (scelta 2)
        match login(&username, &password, &mut write_half, &mut reader).await? {
            Some(user_id) => return Ok((user_id, write_half, reader)),
            None => {
                println!("Riprova.\n");
                continue;
            }
        }
    }
}

/// Acquisisce la scelta dell'utente (1 o 2) dal terminale.
fn read_choice() -> Result<AuthChoice, io::Error> {
    loop {
        print!("Scelta: ");
        io::stdout().flush()?;

        let mut scelta = String::new();
        io::stdin().read_line(&mut scelta)?;

        match scelta.trim() {
            "1" => return Ok(AuthChoice::Login),
            "2" => return Ok(AuthChoice::Register),
            _ => println!("Scelta non valida. Inserisci 1 oppure 2.\n"),
        }
    }
}

/// Richiede username e password dal terminale verificando che non siano stringhe vuote.
fn read_credentials() -> Result<(String, String), io::Error> {
    let username = loop {
        print!("Username: ");
        io::stdout().flush()?;

        let mut username = String::new();
        io::stdin().read_line(&mut username)?;
        let username = username.trim().to_string();

        if username.is_empty() {
            println!("Lo username non può essere vuoto.\n");
            continue;
        }
        break username;
    };

    let password = loop {
        print!("Password: ");
        io::stdout().flush()?;

        let mut password = String::new();
        io::stdin().read_line(&mut password)?;
        let password = password.trim().to_string();

        if password.is_empty() {
            println!("La password non può essere vuota.\n");
            continue;
        }
        break password;
    };

    Ok((username, password))
}

/// Invia al server la richiesta di registrazione e attende l'esito.
async fn register(
    username: &str,
    password: &str,
    write_half: &mut OwnedWriteHalf,
    reader: &mut BufReader<OwnedReadHalf>,
) -> Result<bool, Box<dyn Error>> {
    let reg_msg = Message::RegisterRequest {
        username: username.to_string(),
        password: password.to_string(),
    };

    let json = serde_json::to_string(&reg_msg)? + "\n";
    write_half.write_all(json.as_bytes()).await?;

    let mut line = String::new();
    let bytes_read = reader.read_line(&mut line).await?;

    if bytes_read == 0 {
        eprintln!("Connessione chiusa durante la registrazione.");
        return Ok(false);
    }

    match serde_json::from_str::<Message>(&line)? {
        Message::RegisterResponse { success, message } => {
            println!("Server: {}", message);
            Ok(success)
        }
        _ => {
            eprintln!("Risposta inattesa durante la registrazione.");
            Ok(false)
        }
    }
}

/// Invia al server la richiesta di login e restituisce lo `user_id` assegnato in caso di successo.
async fn login(
    username: &str,
    password: &str,
    write_half: &mut OwnedWriteHalf,
    reader: &mut BufReader<OwnedReadHalf>,
) -> Result<Option<String>, Box<dyn Error>> {
    let login_msg = Message::LoginRequest {
        username: username.to_string(),
        password: password.to_string(),
    };

    let json = serde_json::to_string(&login_msg)? + "\n";
    write_half.write_all(json.as_bytes()).await?;

    let mut line = String::new();
    let bytes_read = reader.read_line(&mut line).await?;

    if bytes_read == 0 {
        eprintln!("Connessione chiusa durante il login.");
        return Ok(None);
    }

    match serde_json::from_str::<Message>(&line)? {
        Message::LoginResponse {
            success: true,
            user_id: Some(user_id),
            message,
        } => {
            println!("Server: {}", message);
            println!("Accesso effettuato correttamente.\n");
            Ok(Some(user_id))
        }
        Message::LoginResponse {
            success: false,
            message,
            ..
        } => {
            eprintln!("Errore di login: {}", message);
            Ok(None)
        }
        Message::LoginResponse {
            success: true,
            user_id: None,
            ..
        } => {
            eprintln!("Login riuscito ma il server non ha restituito un user_id.");
            Ok(None)
        }
        _ => {
            eprintln!("Risposta inattesa durante il login.");
            Ok(None)
        }
    }
}