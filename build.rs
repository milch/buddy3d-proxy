fn main() -> std::io::Result<()> {
    println!("cargo:rerun-if-changed=proto");
    prost_build::compile_protos(&["proto/buddy3d.proto"], &["proto"])?;
    Ok(())
}
