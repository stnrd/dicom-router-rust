//! Interop against the reference dicom-rs CLI tools.
//! Requires `cargo install dicom-storescu` (with TLS features). Ignored by default.

mod common;

#[test]
#[ignore = "requires dicom-storescu binary on PATH"]
fn interop_with_dicom_storescu() {
    // Manual smoke test:
    // 1. Start router with dev certs and a temp config
    // 2. Write a test .dcm via common::test_object
    // 3. dicom-storescu --called-ae-title RUST_ROUTER --tls-ca-file dev-certs/ca.crt <file> 127.0.0.1:2762
    // 4. Assert object appears in queue directory
    let _ = common::generate_pki();
}
