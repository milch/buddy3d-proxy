//! Dev-only task runner. Invoke via `cargo xtask <command>`.
//!
//! Available commands:
//!   gen-proto    Regenerate src/proto/buddy3d.rs from proto/*.proto.

use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let cmd = args.get(1).map(String::as_str).unwrap_or("help");
    let result = match cmd {
        "gen-proto" => gen_proto(),
        "help" | "--help" | "-h" => {
            print_help();
            return ExitCode::SUCCESS;
        }
        other => {
            eprintln!("xtask: unknown command `{other}`");
            print_help();
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("xtask: {e}");
            ExitCode::FAILURE
        }
    }
}

fn print_help() {
    eprintln!("Usage: cargo xtask <command>");
    eprintln!();
    eprintln!("Commands:");
    eprintln!("  gen-proto    Regenerate src/proto/buddy3d.rs from proto/*.proto");
}

fn gen_proto() -> Result<(), Box<dyn std::error::Error>> {
    let workspace_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .ok_or("xtask manifest has no parent directory")?
        .to_path_buf();
    let proto_dir = workspace_root.join("proto");
    let out_dir = workspace_root.join("src/proto");
    println!(
        "regenerating {} from {}/*.proto",
        out_dir.display(),
        proto_dir.display()
    );
    prost_build::Config::new()
        .out_dir(&out_dir)
        .compile_protos(&[proto_dir.join("buddy3d.proto")], &[proto_dir])?;
    println!("✓ wrote {}/buddy3d.rs", out_dir.display());
    Ok(())
}
