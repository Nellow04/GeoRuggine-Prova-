use std::io::{self, Write};
use std::error::Error;

use tokio::io::{
    AsyncBufReadExt,
    AsyncWriteExt,
    BufReader,
};

use tokio::net::TcpStream;
use tokio::net::tcp::{
    OwnedReadHalf,
    OwnedWriteHalf,
};

use shared::Message;


// Tipo restituito dopo un'autenticazione
pub type AuthResult = (
    String,
    OwnedWriteHalf,
    BufReader<OwnedReadHalf>,
);


// Opzioni del menu
enum AuthChoice {
    Login,
    Register,
}


// Funzione iniziale di autenticazione
pub async fn authenticate() -> Result<AuthResult, Box<dyn Error>> {

    loop {

        println!("=== GeoRuggine ===");
        println!("1) Login");
        println!("2) Registrazione");

        let choice = read_choice()?;

        let (username, password) = read_credentials()?;

        // Connessione al server
        let stream = match TcpStream::connect("127.0.0.1:8080").await {
            Ok(stream) => stream,

            Err(e) => {
                eprintln!(
                    "Impossibile connettersi al server: {}. Riprova.\n",
                    e
                );

                continue;
            }
        };

        println!("Connesso al server.");


        // Separiamo lettura e scrittura della connessione
        let (read_half, mut write_half) = stream.into_split();
        let mut reader = BufReader::new(read_half);

        if let AuthChoice::Register = choice {
            let registration_success =
                register(
                    &username,
                    &password,
                    &mut write_half,
                    &mut reader,
                )
                    .await?;

            if !registration_success {
                println!("Riprova.\n");
                continue;
            }

            println!("Login automatico...\n");
        }


        // Il login viene effettuato:
        //
        // - direttamente se l'utente ha scelto Login
        // - automaticamente dopo una registrazione riuscita

        match login(
            &username,
            &password,
            &mut write_half,
            &mut reader,
        )
            .await?
        {
            Some(user_id) => {

                return Ok((
                    user_id,
                    write_half,
                    reader,
                ));
            }

            None => {
                println!("Riprova.\n");
                continue;
            }
        }
    }
}


fn read_choice() -> Result<AuthChoice, io::Error> {

    loop {

        print!("Scelta: ");
        io::stdout().flush()?;

        let mut scelta = String::new();
        io::stdin().read_line(&mut scelta)?;

        match scelta.trim() {

            "1" => {
                return Ok(AuthChoice::Login);
            }

            "2" => {
                return Ok(AuthChoice::Register);
            }

            _ => {
                println!(
                    "Scelta non valida. Inserisci 1 oppure 2.\n"
                );
            }
        }
    }
}

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

    let json =  serde_json::to_string(&reg_msg)? + "\n";

    // Invio richiesta al server
    write_half
        .write_all(json.as_bytes())
        .await?;


    let mut line = String::new();

    let bytes_read =
        reader.read_line(&mut line).await?;

    if bytes_read == 0 {
        eprintln!(
            "Connessione chiusa durante la registrazione."
        );

        return Ok(false);
    }


    match serde_json::from_str::<Message>(&line)? {

        Message::RegisterResponse {
            success,
            message,
        } => {

            println!("Server: {}", message);

            Ok(success)
        }

        _ => {

            eprintln!(
                "Risposta inattesa durante la registrazione."
            );

            Ok(false)
        }
    }
}


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

    let json =
        serde_json::to_string(&login_msg)? + "\n";

    // Invio LoginRequest
    write_half
        .write_all(json.as_bytes())
        .await?;


    // Attendo LoginResponse
    let mut line = String::new();

    let bytes_read =
        reader.read_line(&mut line).await?;

    if bytes_read == 0 {

        eprintln!(
            "Connessione chiusa durante il login."
        );

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

            eprintln!(
                "Errore di login: {}",
                message
            );

            Ok(None)
        }


        Message::LoginResponse {
            success: true,
            user_id: None,
            ..
        } => {

            eprintln!(
                "Login riuscito ma il server non ha restituito un user_id."
            );

            Ok(None)
        }


        _ => {

            eprintln!(
                "Risposta inattesa durante il login."
            );

            Ok(None)
        }
    }
}