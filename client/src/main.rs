mod auth;
mod cli;
mod gps;
mod session;

use std::error::Error;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    /*
     * CICLO PRINCIPALE DELLE SESSIONI
     *
     * Ogni iterazione gestisce un intero ciclo di vita:
     * 1. Autenticazione: selezione Login/Registrazione e connessione TCP
     * 2. Sessione attiva: esecuzione concorrente di GPS, console utente e ricezione server
     * 3. Logout: terminazione ordinata dei task e chiusura connessione
     *
     * Dopo il logout, il ciclo ricomincia automaticamente ripresentando il menu iniziale.
     */
    loop {
        // 1. Autenticazione iniziale (bloccante finché l'utente non fa login con successo)
        let auth_result = auth::authenticate().await?;

        // 2. Esecuzione della sessione attiva (termina alla conferma del logout o disconnessione)
        session::run_session(auth_result).await?;
    }
}