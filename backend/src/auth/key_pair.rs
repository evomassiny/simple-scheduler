use crate::auth::Credentials;
use crate::rocket::tokio::{self, fs::File, io::AsyncReadExt};
use std::path::Path;

/// App-wide auth conf
pub struct KeyPair {
    /// PKCS8 RSA DER private key
    public: Vec<u8>,
    /// PKCS8 RSA DER public key
    private: Vec<u8>,
}
impl KeyPair {
    /// Load keys from 2 files
    pub async fn load_from<P: AsRef<Path>>(
        public_key_path: P,
        private_key_path: P,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let mut public: Vec<u8> = Vec::new();
        let mut public_file = File::open(public_key_path).await?;
        public_file.read_to_end(&mut public).await?;

        let mut private: Vec<u8> = Vec::new();
        let mut private_file = File::open(private_key_path).await?;
        private_file.read_to_end(&mut private).await?;

        Ok(Self { public, private })
    }

    pub fn decode_credentials(
        &self,
        encrypted_data: &str,
    ) -> Result<Credentials, Box<dyn std::error::Error>> {
        Credentials::from_encrypted_str(encrypted_data, &self.private)
    }
}

#[tokio::test]
async fn test_credentials_decoding() {
    const PRIVATE_KEY_PATH: &str = "./test-data/authentification/private.rsa.pkcs8.der";
    const PUBLIC_KEY_PATH: &str = "./test-data/authentification/public.rsa.pkcs8.der";
    const AUTH_DATA: &str =
        include_str!("../../test-data/authentification/credential-java-debug.enc");

    let pair: KeyPair = KeyPair::load_from(&PUBLIC_KEY_PATH, &PRIVATE_KEY_PATH)
        .await
        .expect("Could not load key pair");

    let credential: Credentials = pair
        .decode_credentials(AUTH_DATA)
        .expect("Could not decode auth data.");

    assert_eq!(&credential.login, "debug-user");
    assert_eq!(&credential.pass, "debug-password");
}
