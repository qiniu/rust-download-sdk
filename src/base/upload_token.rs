use super::{credential::Credential, upload_policy::UploadPolicy};

pub(crate) fn sign_upload_token(credential: &Credential, policy: &UploadPolicy) -> String {
    let serialized_policy = policy.to_json();
    credential.sign_with_data(serialized_policy.as_bytes())
}
