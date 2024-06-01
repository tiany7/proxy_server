use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let out_dir = PathBuf::from("/home/ubuntu/proxy_server/proto");
    let generated_code_path = out_dir.join("generated_code");
    let _ = std::fs::create_dir(generated_code_path.clone());
    tonic_build::configure()
        .out_dir(generated_code_path)
        .compile(&["proto/trade.proto"], &["proto"])?;
    Ok(())
}
