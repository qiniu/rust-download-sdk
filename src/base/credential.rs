use hmac::{Hmac, Mac, NewMac};
use sha1::Sha1;

use super::base64;

/// 七牛凭证，用于设置 Access Key 和 Secret Key 以访问私有空间的七牛对象
#[derive(Debug, Clone)]
pub struct Credential {
    access_key: String,
    secret_key: String,
}

impl Credential {
    #[inline]
    /// 创建七牛凭证
    /// # Arguments
    /// * `ak` - 七牛 Access Key
    /// * `sk` - 七牛 Secret Key
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

    pub(crate) fn sign_with_data(&self, data: &[u8]) -> String {
        let encoded_data = base64::urlsafe(data);
        self.sign(encoded_data.as_bytes()) + ":" + &encoded_data
    }

    fn base64_hmac_digest(&self, data: &[u8]) -> String {
        let mut hmac = Hmac::<Sha1>::new_from_slice(self.secret_key.as_bytes()).unwrap();
        hmac.update(data);
        base64::urlsafe(&hmac.finalize().into_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{boxed::Box, error::Error, result::Result, sync::Arc, thread};

    #[test]
    fn test_sign() -> Result<(), Box<dyn Error>> {
        env_logger::try_init().ok();

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
