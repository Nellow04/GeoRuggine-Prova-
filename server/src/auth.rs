use std::collections::HashMap;
use std::fs;

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

pub fn load_accounts() -> HashMap<String, String> {
    if let Ok(content) = fs::read_to_string("accounts.json") {
        if let Ok(accounts) = serde_json::from_str(&content) {
            return accounts;
        }
    }

    HashMap::new()
}


// ============================================================
// SALVATAGGIO ACCOUNT
// ============================================================

pub fn save_accounts(accounts: &HashMap<String, String>) -> Result<(), Box<dyn std::error::Error>> {
    let content = serde_json::to_string_pretty(accounts)?;
    fs::write("accounts.json", content)?;
    Ok(())
}


pub fn hash_password(
    password: &str,
) -> Result<String, argon2::password_hash::Error> {

    let salt = SaltString::generate(&mut OsRng);

    let argon2 = Argon2::default();

    // Il risultato viene salvato in formato PHC: contiene algoritmo, parametri, salt e hash
    let password_hash = argon2
        .hash_password(
                password.as_bytes(),
                &salt,
            )?
            .to_string();

    Ok(password_hash)
}


pub fn verify_password(
    password: &str,
    stored_hash: &str,
) -> bool {

    let parsed_hash =
        match PasswordHash::new(stored_hash) {
            Ok(hash) => hash,
            Err(_) => {
                return false;
            }
        };

    Argon2::default()
        .verify_password(
            password.as_bytes(),
            &parsed_hash,
        )
        .is_ok()
}