use helidb::utils::Result;

fn try_main() -> Result<()> {
    Ok(())
}


fn main() {
    if let Err(e) = try_main() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
