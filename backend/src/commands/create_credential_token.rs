use crate::auth::{Credentials, KeyPair};
use crate::config::Config;
use crate::models::User;

/// Create a credential Token
pub async fn create_credential_token(
    name: &str,
    password: &str,
    config: &Config,
) -> Result<(), String> {
    // verify that the user exist and is authorized,
    let conn = &mut config
        .database_connection()
        .await
        .map_err(|e| format!("failed to get database pool: {:?}", e))?;
    let user = User::get_from_name(name, conn)
        .await
        .ok_or("Unknown user".to_string())?;

    if user.verify_password(password).is_err() {
        return Err(String::from("wrong password."));
    }
    // Load RSA key to encrypt the token data
    let key_pair = KeyPair::load_from(&config.public_key_path, &config.private_key_path)
        .await
        .or(Err("Could not read key pair".to_string()))?;

    // serialize it into a token
    let credentials = Credentials {
        login: name.to_string(),
        pass: password.to_string(),
    };
    let token = credentials
        .into_credential_token(&key_pair.public)
        .map_err(|e| format!("Failed to build auth token {:?}", e))?;
    println!("{}", token);

    Ok(())
}
