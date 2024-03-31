fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .out_dir("./proto/generated_code")
        .compile(
            &[
                "proto/trade.proto",
            ],
            &["proto"],
        )?;
    Ok(())
}