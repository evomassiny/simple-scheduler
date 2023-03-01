use crate::models::{User, UserId};
use aes::{
    cipher::{
        generic_array::GenericArray, 
        BlockDecrypt, 
        BlockEncrypt,
        KeyInit, 
    },
    Aes128, Block,
};

use jaded::{FromJava, Parser};
use rand::{self, Rng};
use rsa::{
    pkcs8::{FromPrivateKey, FromPublicKey},
    PaddingScheme, PublicKey, RsaPrivateKey, RsaPublicKey,
};
use serde::{Deserialize, Serialize};
use serde_json;
use sqlx::SqliteConnection;

use std::io::{Error as IOError, ErrorKind};
use std::str::FromStr;

#[derive(Debug, FromJava, Serialize, Deserialize)]
pub struct Credentials {
    pub login: String,
    pub pass: String,
}

pub const BLOCK_SIZE: usize = 16;

impl Credentials {
    /// Build a Credential from a base64
    /// encoded and AES encrypted str.
    /// The AES key must be stored in the data,
    /// and being decrytable using the PKCS8 RSA private key (in DER format).
    pub fn from_encrypted_str(
        encrypted_data: &str,
        private_key: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let blob = base64::decode(encrypted_data)?;
        let clear_data = EncryptedData::parse(&blob)?.decrypt_data(private_key)?;

        // Try to interpret the bytes as json
        if let Ok(cred) = serde_json::from_slice::<Self>(clear_data.as_slice()) {
            return Ok(cred);
        }
        // then try the legacy format,
        // eg: decode a serialized Java Object
        if let Ok(mut parser) = Parser::new(clear_data.as_slice()) {
            let cred: Self = parser.read_as()?;
            return Ok(cred);
        }

        Err(Box::new(IOError::new(
            ErrorKind::InvalidData,
            "Could not parse auth data",
        )))
    }

    /// test user/pass credentials
    pub async fn get_user(&self, conn: &mut SqliteConnection) -> Option<User<UserId>> {
        if let Some(user) = User::get_from_name(&self.login, conn).await {
            return match user.verify_password(&self.pass) {
                Ok(true) => Some(user),
                _ => None,
            };
        }
        None
    }

    /// Serialize credential into a the original "proactive" token format,
    /// eg:
    /// ```
    /// RSA\n
    /// $AES_KEY_SIZE\n
    /// $AES_KEY_CIPHER\n
    /// $ENCRYPTED_AES_KEY_BYTES
    /// $DATA_ENCRYPTED_WITH_AES_KEY_BYTES
    /// ```
    /// (the only AES_KEY_CIPHER supported beeing "RSA/ECB/PKCS1Padding")
    ///
    /// Then base64 encode it.
    ///
    pub fn into_credential_token(
        &self,
        public_key: &[u8],
    ) -> Result<String, Box<dyn std::error::Error>> {
        // serialize payload as json
        let mut payload: Vec<u8> = serde_json::to_string(self)?.into_bytes();
        while payload.len() % BLOCK_SIZE != 0 {
            // pad json with spaces until
            // it fits on a whole number of AES blocks
            payload.push(b' ');
        }
        // create a 128 bytes AES key,
        let mut rng = rand::thread_rng();
        let mut aes_key: [u8; 16] = [0; 16];
        for byte in &mut aes_key {
            *byte = rng.gen();
        }
        // encrypt the payload using the AES key, block
        let encrypted_payload: Vec<u8> = {
            let aes_key = GenericArray::from_slice(&aes_key);
            let cipher = Aes128::new(aes_key);

            let mut block_start = 0;
            let mut encrypted_payload: Vec<u8> = Vec::new();
            while block_start < payload.len() {
                // safe because we padded the byte string until
                // its length reached a factor of `BLOCK_SIZE`
                let mut block =
                    *Block::from_slice(&payload[block_start..(block_start + BLOCK_SIZE)]);
                block_start += BLOCK_SIZE;
                cipher.encrypt_block(&mut block);
                encrypted_payload.extend_from_slice(block.as_mut_slice());
            }
            encrypted_payload
        };
        // encrypt the AES key using RSA public key
        let rsa_public_key = RsaPublicKey::from_public_key_der(public_key)?;
        let padding_scheme = PaddingScheme::new_pkcs1v15_encrypt();
        let encrypted_aes_key = rsa_public_key.encrypt(&mut rng, padding_scheme, &aes_key)?;

        // gather all this into a byte string
        let mut blob: Vec<u8> = Vec::new();
        blob.extend(b"RSA\n");
        blob.extend(b"1024\n"); // size of AES KEY
        blob.extend(b"RSA/ECB/PKCS1Padding\n"); // how is the AES key encrypted
        blob.extend(encrypted_aes_key); // the AES key
        blob.extend(encrypted_payload); // The payload
                                        //
                                        // encode the bytes into a string
        let token = base64::encode(blob);
        Ok(token)
    }
}

/// return the index of the first occurence of `target` in `bytes`
fn index_of(bytes: &[u8], target: u8) -> Result<usize, IOError> {
    let mut idx = 0;
    loop {
        if idx >= bytes.len() {
            return Err(IOError::new(ErrorKind::UnexpectedEof, "End of input"));
        }
        if bytes[idx] == target {
            return Ok(idx);
        }
        idx += 1;
    }
}

/// Cipher used by proactive auth tokens
#[allow(non_camel_case_types)]
pub enum CipherKind {
    /// Only supported cipher kind
    RSA_ECB_PKCS1Padding,
}
impl FromStr for CipherKind {
    type Err = IOError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "RSA/ECB/PKCS1Padding" {
            return Ok(Self::RSA_ECB_PKCS1Padding);
        }
        Err(IOError::new(
            ErrorKind::InvalidData,
            "unsuported cipher kind",
        ))
    }
}

/// Proactive's auth token
pub struct EncryptedData<'a> {
    //algo: &'a str,
    //aes_encrypted_key_size: usize,
    cipher: CipherKind,
    /// AES key, encrypted using the server private key
    encrypted_aes_key: &'a [u8],
    /// data encrypted with the AES key
    encrypted_data: &'a [u8],
}
impl<'a> EncryptedData<'a> {
    /// Parse a bytes slice into an EncryptedData,
    /// but does not decrypt it.
    ///
    /// The data is expected to follow the original "proactive" token format,
    /// eg:
    /// ```
    /// RSA\n
    /// $AES_KEY_SIZE\n
    /// $AES_KEY_CIPHER\n
    /// $ENCRYPTED_AES_KEY\n
    /// $DATA_ENCRYPTED_WITH_AES_KEY
    /// ```
    /// (the only AES_KEY_CIPHER supported beeing "RSA/ECB/PKCS1Padding")
    ///
    ///
    pub fn parse(bytes: &'a [u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut start: usize = 0;
        let mut idx: usize = 0;
        // parse algo field
        idx += index_of(&bytes[start..], b'\n')?;
        let _algo: &str = std::str::from_utf8(&bytes[start..idx])?;
        // parse size
        start = idx + 1; // skip '\n'
        idx = start + index_of(&bytes[start..], b'\n')?;
        let size_str: &str = std::str::from_utf8(&bytes[start..idx])?;
        let aes_encrypted_key_size: usize = size_str.parse::<usize>()?;
        // parse cipher
        start = idx + 1; // skip '\n'
        idx = start + index_of(&bytes[start..], b'\n')?;
        let cipher: CipherKind = std::str::from_utf8(&bytes[start..idx])?.parse::<CipherKind>()?;
        // parse AES key, it's must be `size` bits
        start = idx + 1; // skip '\n'
        idx = start + aes_encrypted_key_size / 8usize;
        if idx > bytes.len() {
            return Err(Box::new(IOError::new(
                ErrorKind::UnexpectedEof,
                "key too long",
            )));
        }
        let encrypted_aes_key = &bytes[start..idx];
        // the rest of the file is data encrypted with the (decoded) AES key
        let encrypted_data = &bytes[idx..];
        Ok(Self {
            cipher,
            encrypted_aes_key,
            encrypted_data,
        })
    }

    /// Decrypts the aes key using an RSA PKCS8 DER private key.
    fn decrypt_aes_key(&self, private_key: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        match self.cipher {
            CipherKind::RSA_ECB_PKCS1Padding => {
                let rsa_private_key = RsaPrivateKey::from_pkcs8_der(private_key)?;
                let padding_scheme = PaddingScheme::new_pkcs1v15_encrypt();
                Ok(rsa_private_key.decrypt(padding_scheme, self.encrypted_aes_key)?)
            }
            _ => Err(Box::new(IOError::new(
                ErrorKind::InvalidInput,
                "Unsupported cipher",
            ))),
        }
    }

    /// Decrypts the EncryptedData content using an RSA PKCS8 DER key.
    pub fn decrypt_data(&self, private_key: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let aes_key_bytes: Vec<u8> = self.decrypt_aes_key(private_key)?;
        let aes_key = GenericArray::from_slice(&aes_key_bytes[..16]);
        let cipher = Aes128::new(aes_key);
        let mut block_start = 0;
        let mut decrypted: Vec<u8> = Vec::new();
        if self.encrypted_data.len() % BLOCK_SIZE != 0 {
            return Err(Box::new(IOError::new(
                ErrorKind::InvalidInput,
                "Encrypted data is not padded",
            )));
        }
        while block_start < self.encrypted_data.len() {
            let mut block =
                *Block::from_slice(&self.encrypted_data[block_start..(block_start + BLOCK_SIZE)]);
            block_start += BLOCK_SIZE;
            cipher.decrypt_block(&mut block);
            decrypted.extend_from_slice(block.as_mut_slice());
        }
        Ok(decrypted)
    }
}

#[test]
fn test_java_credentials_deserialization() {
    const PRIVATE_KEY: &[u8] =
        include_bytes!("../../test-data/authentification/private.rsa.pkcs8.der");
    const AUTH_DATA: &str =
        include_str!("../../test-data/authentification/credential-java-debug.enc");

    let credential: Credentials =
        Credentials::from_encrypted_str(AUTH_DATA, PRIVATE_KEY).expect("Could not parse token");
    assert_eq!(&credential.login, "debug-user");
    assert_eq!(&credential.pass, "debug-password");
}

#[test]
fn test_json_credentials_deserialization() {
    const PRIVATE_KEY: &[u8] =
        include_bytes!("../../test-data/authentification/private.rsa.pkcs8.der");
    const PUB_KEY: &[u8] = include_bytes!("../../test-data/authentification/public.rsa.pkcs8.der");

    let credential = Credentials {
        login: "some_name".to_string(),
        pass: "some_pass".to_string(),
    };
    let token = credential
        .into_credential_token(PUB_KEY)
        .expect("Failed to produce token");
    let should_be_same: Credentials =
        Credentials::from_encrypted_str(&token, PRIVATE_KEY).expect("Could not parse token");
    assert_eq!(&credential.login, &should_be_same.login);
    assert_eq!(&credential.pass, &should_be_same.pass);
}
