pub fn parse_sse_data(buffer: &mut String, bytes: &[u8]) -> Vec<String> {
    buffer.push_str(&String::from_utf8_lossy(bytes));
    let mut events = Vec::new();
    while let Some(index) = buffer.find("\n\n") {
        let frame = buffer.drain(..index + 2).collect::<String>();
        let data = frame
            .lines()
            .filter_map(|line| line.strip_prefix("data:"))
            .map(str::trim_start)
            .collect::<Vec<_>>()
            .join("\n");
        if !data.is_empty() {
            events.push(data);
        }
    }
    events
}
