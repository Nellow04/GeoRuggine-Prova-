use server::auth::{hash_password, verify_password};

#[test]
fn test_hash_and_verify_correct_password() {
    let password = "MySecretPassword123!";
    let hash = hash_password(password).expect("Hashing should succeed");
    assert!(verify_password(password, &hash), "Password verification should succeed");
}

#[test]
fn test_verify_wrong_password() {
    let password = "CorrectPassword";
    let wrong_password = "WrongPassword";
    let hash = hash_password(password).expect("Hashing should succeed");
    assert!(!verify_password(wrong_password, &hash), "Wrong password verification should fail");
}

#[test]
fn test_salt_uniqueness() {
    let password = "SamePassword";
    let hash1 = hash_password(password).expect("Hashing 1 should succeed");
    let hash2 = hash_password(password).expect("Hashing 2 should succeed");
    assert_ne!(hash1, hash2, "Hashes of same password must differ due to random salt");
    assert!(verify_password(password, &hash1));
    assert!(verify_password(password, &hash2));
}

#[test]
fn test_verify_invalid_hash() {
    assert!(!verify_password("test", "invalid_hash_string"));
    assert!(!verify_password("test", ""));
}
