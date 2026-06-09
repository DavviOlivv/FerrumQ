use std::process::Command;

#[test]
fn version_flag_reports_brokerd_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_brokerd"))
        .arg("--version")
        .output()
        .unwrap();

    assert!(output.status.success());

    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.starts_with("brokerd "));
}
