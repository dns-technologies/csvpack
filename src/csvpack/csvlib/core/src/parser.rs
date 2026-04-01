use std::io::Read;
use encoding_rs::Encoding;

use crate::constants::CHUNK_SIZE;


pub struct ReaderState {
    pub buffer: Vec<u8>,
    pub pos_in_buffer: usize,
    pub eof: bool,
}


impl Default for ReaderState {
    fn default() -> Self {
        Self {
            buffer: Vec::with_capacity(CHUNK_SIZE),
            pos_in_buffer: 0,
            eof: false,
        }
    }
}


pub struct CsvParser {
    pub delimiter: u8,
    pub quote_char: u8,
    pub encoding: &'static Encoding,
}


impl CsvParser {
    pub fn new(
        delimiter: u8,
        quote_char: u8,
        encoding: &'static Encoding,
    ) -> Self {
        CsvParser {
            delimiter,
            quote_char,
            encoding,
        }
    }

pub fn read_row_from_buffer<R: Read>(
    &self,
    reader: &mut R,
    state: &mut ReaderState,
) -> Result<Option<Vec<String>>, String> {
    let mut row = Vec::new();
    let mut field = Vec::new();
    let mut in_quotes = false;
    let mut at_start = true;

    loop {
        if state.pos_in_buffer >= state.buffer.len() {
            if state.eof {
                if !field.is_empty() || !at_start {
                    row.push(self.field_to_string(&field));
                }
                if row.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(row));
            }

            state.buffer.clear();
            state.buffer.resize(CHUNK_SIZE, 0);

            match reader.read(&mut state.buffer[..]) {
                Ok(0) => {
                    state.eof = true;
                    state.buffer.clear();
                    if !field.is_empty() || !at_start {
                        row.push(self.field_to_string(&field));
                    }
                    if row.is_empty() {
                        return Ok(None);
                    }
                    return Ok(Some(row));
                }
                Ok(n) => {
                    state.buffer.truncate(n);
                    state.pos_in_buffer = 0;
                }
                Err(e) => return Err(format!("Read error: {}", e)),
            }
        }

        let byte = state.buffer[state.pos_in_buffer];
        state.pos_in_buffer += 1;
        at_start = false;

        if byte == self.quote_char {
            if in_quotes {
                if state.pos_in_buffer < state.buffer.len() &&
                    state.buffer[state.pos_in_buffer] == self.quote_char {
                    field.push(self.quote_char);
                    state.pos_in_buffer += 1;
                    continue;
                } else {
                    in_quotes = false;
                    continue;
                }
            } else {
                in_quotes = true;
                continue;
            }
        }

        if !in_quotes && byte == self.delimiter {
            row.push(self.field_to_string(&field));
            field.clear();
            continue;
        }

        if !in_quotes && (byte == b'\n' || byte == b'\r') {
            if byte == b'\r' {
                continue;
            }

            row.push(self.field_to_string(&field));
            field.clear();

            return Ok(Some(row));
        }

        field.push(byte);
    }
}
    fn field_to_string(&self, field: &[u8]) -> String {
        if field.is_empty() {
            return String::new();
        }

        let (cow, _, _) = self.encoding.decode(field);
        cow.to_string()
    }
}
