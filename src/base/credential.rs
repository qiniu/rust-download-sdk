use crypto_mac::Mac;
use hmac::Hmac;
use sha1::Sha1;

use super::base64;

pub struct Credential {
    access_key: String,
    secret_key: String,
}

impl Credential {
    #[inline]
    pub fn new(ak: impl Into<String>, sk: impl Into<String>) -> Credential {
        Credential {
            access_key: ak.into(),
            secret_key: sk.into(),
        }
    }

    #[inline]
    pub(crate) fn access_key(&self) -> &str {
        &self.access_key
    }

    pub(crate) fn sign(&self, data: &[u8]) -> String {
        self.access_key.to_owned() + ":" + &self.base64_hmac_digest(data)
    }

    fn base64_hmac_digest(&self, data: &[u8]) -> String {
        let mut hmac = Hmac::<Sha1>::new_varkey(self.secret_key.as_bytes()).unwrap();
        hmac.input(data);
        base64::urlsafe(&hmac.result().code())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{boxed::Box, error::Error, result::Result, sync::Arc, thread};

    #[test]
    fn test_sign() -> Result<(), Box<dyn Error>> {
        let credential = Arc::new(Credential::new("abcdefghklmnopq", "1234567890"));
        let mut threads = Vec::new();
        {
            threads.push(thread::spawn(move || {
                assert_eq!(
                    credential.sign(b"hello"),
                    "abcdefghklmnopq:b84KVc-LroDiz0ebUANfdzSRxa0="
                );
                assert_eq!(
                    credential.sign(b"world"),
                    "abcdefghklmnopq:VjgXt0P_nCxHuaTfiFz-UjDJ1AQ="
                );
            }));
        }
        {
            let credential = Arc::new(Credential::new("abcdefghklmnopq", "1234567890"));
            threads.push(thread::spawn(move || {
                assert_eq!(
                    credential.sign(b"-test"),
                    "abcdefghklmnopq:vYKRLUoXRlNHfpMEQeewG0zylaw="
                );
                assert_eq!(
                    credential.sign(b"ba#a-"),
                    "abcdefghklmnopq:2d_Yr6H1GdTKg3RvMtpHOhi047M="
                );
            }));
        }
        threads
            .into_iter()
            .for_each(|thread| thread.join().unwrap());
        Ok(())
    }
}
