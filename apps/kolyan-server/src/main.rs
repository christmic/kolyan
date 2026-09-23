fn main() {
    use kolyan_ledger::FileLedger;
    use std::io::{BufRead, Write};

    let ledger_path = std::env::var_os("KOLYAN_LEDGER_PATH")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(".kolyan/ledger.jsonl"));
    let ledger = FileLedger::open(ledger_path).expect("server ledger should open");
    let server = kolyan_server::ExecutionServer::new(ledger);
    let stdin = std::io::stdin();
    let mut stdout = std::io::BufWriter::new(std::io::stdout());
    for line in stdin.lock().lines() {
        let line = line.expect("server stdin should be readable");
        writeln!(stdout, "{}", server.handle_json_rpc(&line)).expect("server stdout should write");
        stdout.flush().expect("server stdout should flush");
    }
}
