use crate::scheduling::SchedulerClient;
use crate::sqlx::Row;
use rocket::{form::Form, fs::TempFile, State};
use rsa::{PaddingScheme, RsaPrivateKey, pkcs8::FromPrivateKey};
use sqlx::sqlite::SqliteConnection;
use std::io::{Error as IOError, ErrorKind};
use std::str::FromStr;
use aes::{BLOCK_SIZE, Aes128, Block, NewBlockCipher, BlockDecrypt, cipher::generic_array::GenericArray};
//use aes::cipher::{
    ////BlockCipher, BlockEncrypt, BlockDecrypt, NewBlockCipher,
    //generic_array::GenericArray,
//};

fn count_bytes_until(bytes: &[u8], target: u8) -> Result<usize, IOError> {
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
pub struct AuthToken<'a> {
    //algo: &'a str,
    //aes_encrypted_key_size: usize,
    cipher: CipherKind,
    /// AES key, encrypted using the server private key
    encrypted_aes_key: &'a [u8],
    /// data encrypted with the AES key
    encrypted_data: &'a [u8],
}
impl<'a> AuthToken<'a> {
    pub fn parse(bytes: &'a [u8]) -> Result<Self, Box<dyn std::error::Error>> {
        let mut start: usize = 0;
        let mut idx: usize = 0;
        // parse algo field
        idx += count_bytes_until(&bytes[start..], '\n' as u8)?;
        let _algo: &str = std::str::from_utf8(&bytes[start..idx])?;
        // parse size
        start = idx + 1; // skip '\n'
        idx = start + count_bytes_until(&bytes[start..], '\n' as u8)?;
        let size_str: &str = std::str::from_utf8(&bytes[start..idx])?;
        dbg!(&size_str);
        let aes_encrypted_key_size: usize = size_str.parse::<usize>()?;
        // parse cipher
        start = idx + 1; // skip '\n'
        idx = start + count_bytes_until(&bytes[start..], '\n' as u8)?;
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

    pub fn decrypt_aes_key(
        &self,
        private_key: &[u8],
    ) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
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

    pub fn decrypt_data(&self, private_key: &[u8]) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let aes_key_bytes: Vec<u8> = self.decrypt_aes_key(private_key)?;
        let aes_key = GenericArray::from_slice(&aes_key_bytes[..16]);
        let cipher = Aes128::new(&aes_key);
        let mut block_start = 0;
        let mut block_end = BLOCK_SIZE;
        let mut decrypted: Vec<u8> = Vec::new();
        while block_end <= self.encrypted_data.len() {
            let mut block = Block::from_slice(&self.encrypted_data[block_start..block_end]).clone();
            block_start += BLOCK_SIZE;
            block_end += BLOCK_SIZE;
            cipher.decrypt_block(&mut block);
            decrypted.extend_from_slice(block.as_mut_slice());
        }
        Ok(decrypted)
    }
}

#[derive(FromForm)]
pub struct CredentialFileForm<'r> {
    credential: TempFile<'r>,
}

#[post("/login", data = "<credential>")]
pub async fn login(mut credential: Form<CredentialFileForm<'_>>) -> &'static str {
    "TODO"
}

#[test]
fn test_auth() {
    const PRIVATE_KEY: &[u8] = include_bytes!("../../keys/priv.key");
    const AUTH_DATA: &[u8] = include_bytes!("../../keys/mine.decoded");

    let token: AuthToken = AuthToken::parse(AUTH_DATA).expect("Could not parse token");
    let data: Vec<u8> = token.decrypt_data(PRIVATE_KEY).expect("Could not parse token");
    std::fs::write("./serialized_cred_data", &data).expect("could not write data to disk");
    println!("decoded: '{:?}'", &data);
    let data_str = std::str::from_utf8(&data).expect("not utf8");

    println!("decoded: '{}'", &data_str);

}
