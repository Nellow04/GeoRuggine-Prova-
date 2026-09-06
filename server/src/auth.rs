use argon2::{
    password_hash::{rand_core::OsRng, SaltString},
    Argon2, PasswordHash, PasswordHasher, PasswordVerifier,
};
use std::error::Error;

pub type AuthResult<T> = Result<T, Box<dyn Error + Send + Sync>>;

/// Esegue l'hashing della password tramite Argon2 in modo asincrono.
///
/// Argon2 è un algoritmo CPU-bound volutamente oneroso per prevenire attacchi a forza bruta.
/// Viene delegato a `tokio::task::spawn_blocking` per non bloccare i worker thread di Tokio.
pub async fn hash_password(password: &str) -> AuthResult<String> {
    let password = password.to_string();

    tokio::task::spawn_blocking(move || -> AuthResult<String> {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();

        // Il risultato viene salvato in formato PHC: contiene algoritmo, parametri, salt e hash
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)?
            .to_string();

        Ok(password_hash)
    })
    .await?
}

/// Verifica la corrispondenza di una password con l'hash PHC memorizzato in modo asincrono.
///
/// Anche la verifica è computazionalmente onerosa (CPU-bound) e viene quindi
/// eseguita nel pool di thread bloccanti di Tokio.
pub async fn verify_password(password: &str, stored_hash: &str) -> AuthResult<bool> {
    let password = password.to_string();
    let stored_hash = stored_hash.to_string();

    tokio::task::spawn_blocking(move || -> AuthResult<bool> {
        let parsed_hash = PasswordHash::new(&stored_hash)?;

        match Argon2::default().verify_password(password.as_bytes(), &parsed_hash) {
            Ok(()) => Ok(true),

            // La password non corrisponde: errore normale dell'utente
            Err(argon2::password_hash::Error::Password) => Ok(false),

            // Qualsiasi altro errore è un errore interno
            Err(e) => Err(Box::new(e)),
        }
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_async_hash_and_verify_success() {
        let password = "SuperSecretPassword123!";
        let hash = hash_password(password).await.expect("Hashing failed");
        assert!(verify_password(password, &hash).await.unwrap());
    }

    #[tokio::test]
    async fn test_async_verify_wrong_password() {
        let password = "SuperSecretPassword123!";
        let hash = hash_password(password).await.expect("Hashing failed");
        assert!(!verify_password("WrongPassword", &hash).await.unwrap());
    }

    #[tokio::test]
    async fn test_malformed_hash_returns_error() {
        let result = verify_password("password", "questo-non-e-un-hash-phc").await;

        assert!(result.is_err());
    }
}
