use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBytes};
use std::collections::HashMap;
use encoding_rs::Encoding;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::constants::CHUNK_SIZE;
use crate::{is_nan, serialize_to_csv};

struct WriterState {
    delimiter: u8,
    quote_char: u8,
    line_terminator: String,
    column_order: Vec<String>,
    has_header: bool,
    headers_written: bool,
    pos: u64,
    encoding: &'static Encoding,
    pending: Vec<u8>,
}

#[pyclass]
pub struct CsvWriterIterator {
    state: WriterState,
    size_ref: Arc<AtomicU64>,
    finished: bool,
    current_chunk: Option<Vec<u8>>,
    input_data: Vec<Vec<Py<PyAny>>>,
    current_index: usize,
}

#[pymethods]
impl CsvWriterIterator {
    #[new]
    fn new(
        metadata: Option<Vec<HashMap<String, String>>>,
        has_header: Option<bool>,
        delimiter: Option<String>,
        quote_char: Option<String>,
        encoding: Option<String>,
    ) -> PyResult<Self> {
        let delim = delimiter.unwrap_or_else(|| ",".to_string());
        let quote = quote_char.unwrap_or_else(|| "\"".to_string());
        let enc_name = encoding.unwrap_or_else(|| "utf-8".to_string());
        let encoding = crate::get_encoding(&enc_name);
        
        let mut column_order = Vec::new();

        if let Some(meta_list) = metadata {
            for item in meta_list {
                for (col_name, _) in item {
                    if !column_order.contains(&col_name) {
                        column_order.push(col_name.clone());
                    }
                }
            }
        }

        Ok(CsvWriterIterator {
            state: WriterState {
                delimiter: delim.as_bytes()[0],
                quote_char: quote.as_bytes()[0],
                line_terminator: "\n".to_string(),
                column_order,
                has_header: has_header.unwrap_or(true),
                headers_written: false,
                pos: 0,
                encoding,
                pending: Vec::with_capacity(CHUNK_SIZE),
            },
            size_ref: Arc::new(AtomicU64::new(0)),
            finished: false,
            current_chunk: None,
            input_data: Vec::new(),
            current_index: 0,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(
        mut slf: PyRefMut<'_, Self>,
        py: Python<'_>,
    ) -> PyResult<Option<Py<PyBytes>>> {
        if slf.finished {
            return Ok(None);
        }

        if let Some(chunk) = slf.current_chunk.take() {
            return Ok(Some(PyBytes::new(py, &chunk).into()));
        }

        while slf.current_index < slf.input_data.len() {
            // Получаем индекс и значение, разрывая заимствование
            let idx = slf.current_index;
            let row = std::mem::replace(&mut slf.input_data[idx], Vec::new());
            slf.current_index += 1;
            
            let row_bytes = slf.write_row_bytes(py, &row)?;
            slf.state.pending.extend_from_slice(&row_bytes);
            slf.size_ref.store(slf.current_index as u64, Ordering::Relaxed);

            if slf.state.pending.len() >= CHUNK_SIZE {
                let chunk = std::mem::take(&mut slf.state.pending);
                slf.current_chunk = Some(chunk);
                let chunk_bytes = slf.current_chunk.take().unwrap();
                return Ok(Some(PyBytes::new(py, &chunk_bytes).into()));
            }
        }

        slf.finished = true;
        
        if !slf.state.pending.is_empty() {
            let chunk = std::mem::take(&mut slf.state.pending);
            return Ok(Some(PyBytes::new(py, &chunk).into()));
        }
        
        Ok(None)
    }

    fn feed_data(&mut self, py: Python<'_>, rows: &Bound<'_, PyAny>) -> PyResult<()> {
        let iterator = rows.call_method0("__iter__")?;
        
        loop {
            let next_item = match iterator.call_method0("__next__") {
                Ok(item) => item,
                Err(e) => {
                    if e.is_instance_of::<pyo3::exceptions::PyStopIteration>(py) {
                        break;
                    }
                    return Err(e);
                }
            };
            
            let row: Vec<Py<PyAny>> = next_item.extract()?;
            self.input_data.push(row);
        }
        
        Ok(())
    }

    fn tell(&self) -> u64 {
        self.state.pos
    }
}

impl CsvWriterIterator {
    fn write_row_bytes(
        &mut self,
        py: Python<'_>,
        row: &[Py<PyAny>],
    ) -> PyResult<Vec<u8>> {
        let mut row_buffer = Vec::new();

        if !self.state.headers_written && self.state.has_header && !self.state.column_order.is_empty() {
            for (i, col_name) in self.state.column_order.iter().enumerate() {
                if i > 0 {
                    row_buffer.push(self.state.delimiter);
                }
                let (encoded, _, _) = self.state.encoding.encode(col_name);
                row_buffer.extend_from_slice(&encoded);
            }
            row_buffer.extend_from_slice(self.state.line_terminator.as_bytes());
            self.state.headers_written = true;
        }

        for (i, item) in row.iter().enumerate() {
            if i > 0 {
                row_buffer.push(self.state.delimiter);
            }
            
            let item_bound = item.bind(py);
            let item_str = self.value_to_string(py, &item_bound)?;
            row_buffer.extend(self.write_escaped_field(&item_str));
        }

        row_buffer.extend_from_slice(self.state.line_terminator.as_bytes());
        self.state.pos += row_buffer.len() as u64;
        Ok(row_buffer)
    }

    fn value_to_string(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<String> {
        if value.is_none() {
            return Ok(String::new());
        }
        
        if let Ok(f) = value.extract::<f64>() {
            if f.is_nan() {
                return Ok(String::new());
            }
        }
        
        if let Ok(is_nan) = is_nan(py, value) {
            if is_nan {
                return Ok(String::new());
            }
        }
        
        serialize_to_csv(py, value)
    }

    fn write_escaped_field(&self, field: &str) -> Vec<u8> {
        let needs_quoting = field.contains(self.state.delimiter as char)
            || field.contains(self.state.quote_char as char)
            || field.contains('\n')
            || field.contains('\r');
        
        let mut result = Vec::new();

        if needs_quoting {
            result.push(self.state.quote_char);

            for c in field.chars() {
                if c == self.state.quote_char as char {
                    result.push(self.state.quote_char);
                }
                result.extend_from_slice(c.to_string().as_bytes());
            }

            result.push(self.state.quote_char);
        } else {
            result.extend_from_slice(field.as_bytes());
        }

        result
    }
}
