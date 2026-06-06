use clap::Parser;

#[derive(Debug, Parser)]
#[command(
    name = "brokerd",
    version,
    about = "FerrumQ broker daemon placeholder for Milestone 0"
)]
struct Cli;

fn main() {
    let _cli = Cli::parse();
}
