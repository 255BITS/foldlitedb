//! Native protocol driver for parity checks and profiling; not a Python backend.
use std::io::{self, BufRead, Write};

fn main() -> io::Result<()> {
    let mut output = io::BufWriter::new(io::stdout().lock());
    for line in io::stdin().lock().lines() {
        output.write_all(&foldlitedb_reducer::protocol::execute_json(
            line?.as_bytes(),
        ))?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}
