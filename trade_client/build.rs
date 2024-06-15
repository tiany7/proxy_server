use std::path::PathBuf;
use std::fs::{self, OpenOptions};
use std::io::{Write, Seek, SeekFrom, Read};

fn main() -> Result<(), Box<dyn std::error::Error>> {

    let proto_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../proto");

    let generated_code_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("generated_code");


    fs::create_dir_all(&generated_code_dir)?;

    tonic_build::configure()
        .out_dir(&generated_code_dir)
        .compile(
            &[proto_dir.join("trade.proto").to_str().unwrap()],
            &[proto_dir.to_str().unwrap()],
        )?;


    let generated_file_path = generated_code_dir.join("trade.rs");


    let mut file = OpenOptions::new()
        .write(true)
        .read(true)
        .open(&generated_file_path)?;


    let mut content = String::new();
    file.read_to_string(&mut content)?;


    if let Some(pos) = content.find("#![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]") {
        let insert_pos = pos + "#![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]".len();
        let new_content = format!(
            "{}\n{}\n{}",
            &content[..insert_pos],
            "use std::convert::TryInto;",
            &content[insert_pos..]
        );

        file.set_len(0)?; // 清空文件
        file.seek(SeekFrom::Start(0))?;
        file.write_all(new_content.as_bytes())?;
    }

    Ok(())
}
