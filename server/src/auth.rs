

use argon2::{
    Argon2,
    PasswordHasher,
    PasswordVerifier,
    PasswordHash,
    password_hash::{
        SaltString,
        rand_core::OsRng,
    },
};

/// Esegue l'hashing della password tramite Argon2 in modo asincrono.
/// 
/// Argon2 è un algoritmo CPU-bound volutamente oneroso per prevenire attacchi a forza bruta.
/// Viene delegato a `tokio::task::spawn_blocking` per non bloccare i worker thread di Tokio.
pub async fn hash_password(
    password: &str,
) -> Result<String, argon2::password_hash::Error> {
    let password = password.to_string();

    tokio::task::spawn_blocking(move || {
        let salt = SaltString::generate(&mut OsRng);
        let argon2 = Argon2::default();

        // Il risultato viene salvato in formato PHC: contiene algoritmo, parametri, salt e hash
        let password_hash = argon2
            .hash_password(password.as_bytes(), &salt)?
            .to_string();

        Ok(password_hash)
    })
    .await
    .expect("spawn_blocking for hash_password panicked")
}

/// Verifica la corrispondenza di una password con l'hash PHC memorizzato in modo asincrono.
/// 
/// Anche la verifica è computazionalmente onerosa (CPU-bound) e viene quindi
/// eseguita nel pool di thread bloccanti di Tokio.
pub async fn verify_password(
    password: &str,
    stored_hash: &str,
) -> bool {
    let password = password.to_string();
    let stored_hash = stored_hash.to_string();

    tokio::task::spawn_blocking(move || {
        let parsed_hash = match PasswordHash::new(&stored_hash) {
            Ok(hash) => hash,
            Err(_) => {
                return false;
            }
        };

        Argon2::default()
            .verify_password(password.as_bytes(), &parsed_hash)
            .is_ok()
    })
    .await
    .expect("spawn_blocking for verify_password panicked")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_async_hash_and_verify_success() {
        let password = "SuperSecretPassword123!";
        let hash = hash_password(password).await.expect("Hashing failed");
        assert!(verify_password(password, &hash).await);
    }

    #[tokio::test]
    async fn test_async_verify_wrong_password() {
        let password = "SuperSecretPassword123!";
        let hash = hash_password(password).await.expect("Hashing failed");
        assert!(!verify_password("WrongPassword", &hash).await);
    }
}