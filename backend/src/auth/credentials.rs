use crate::models::{User, UserId};
use aes::{
    cipher::{generic_array::GenericArray, BlockDecrypt, KeyInit},
    Aes128, Block,
};

use jaded::{FromJava, Parser};
use rsa::{pkcs8::FromPrivateKey, PaddingScheme, RsaPrivateKey};
use sqlx::SqliteConnection;
use std::io::{Error as IOError, ErrorKind};
use std::str::FromStr;

#[derive(Debug, FromJava)]
pub struct Credentials {
    pub login: String,
    pub pass: String,
}

pub const BLOCK_SIZE: usize = 16;

impl Credentials {
    /// Build a Credential from a base64
    /// encoded, AES encrypted str.
    /// The AES key must be stored in the data,
    /// and being decrytable using the PKCS8 RSA private key (in DER format).
    pub fn from_encrypted_str(
        encrypted_data: &str,
        private_key: &[u8],
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let blob = base64::decode(encrypted_data)?;
        let clear_data = EncryptedData::parse(&blob)?.decrypt_data(private_key)?;

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
        let mut block_end = BLOCK_SIZE;
        let mut decrypted: Vec<u8> = Vec::new();
        while block_end <= self.encrypted_data.len() {
            let mut block = *Block::from_slice(&self.encrypted_data[block_start..block_end]);
            block_start += BLOCK_SIZE;
            block_end += BLOCK_SIZE;
            cipher.decrypt_block(&mut block);
            decrypted.extend_from_slice(block.as_mut_slice());
        }
        Ok(decrypted)
    }
}

#[test]
fn test_credential() {
    const PRIVATE_KEY: &[u8] =
        include_bytes!("../../test-data/authentification/private.rsa.pkcs8.der");
    const AUTH_DATA: &str =
        include_str!("../../test-data/authentification/credential-java-debug.enc");

    let credential: Credentials =
        Credentials::from_encrypted_str(AUTH_DATA, PRIVATE_KEY).expect("Could not parse token");
    assert_eq!(&credential.login, "debug-user");
    assert_eq!(&credential.pass, "debug-password");
}
