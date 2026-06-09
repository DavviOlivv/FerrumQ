use std::process::Command;

fn brokerd() -> Command {
    Command::new(env!("CARGO_BIN_EXE_brokerd"))
}

#[test]
fn version_command_succeeds() {
    let output = brokerd().arg("--version").output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("brokerd"));
}

#[test]
fn serve_help_documents_defaults() {
    let output = brokerd().args(["serve", "--help"]).output().unwrap();

    assert!(output.status.success());
    let stdout = String::from_utf8(output.stdout).unwrap();
    assert!(stdout.contains("--data-dir"));
    assert!(stdout.contains("./.ferrumq"));
    assert!(stdout.contains("--listen"));
    assert!(stdout.contains("127.0.0.1:8080"));
}

#[test]
fn invalid_listen_address_fails_cleanly() {
    let output = brokerd()
        .args(["serve", "--listen", "not-a-socket-address"])
        .output()
        .unwrap();

    assert!(!output.status.success());
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(stderr.contains("invalid value"));
    assert!(stderr.contains("--listen"));
}
