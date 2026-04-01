use pyo3::prelude::*;
use pyo3::types::{PyAny, PyTuple};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::constants::CHUNK_SIZE;
use crate::parser::{CsvParser, ReaderState};
use crate::types::TypeConverter;
use crate::PyReader;


#[pyclass]
pub struct CsvReaderIterator {
    parser: CsvParser,
    type_converter: TypeConverter,
    metadata: HashMap<String, String>,
    column_order: Vec<String>,
    has_header: bool,
    headers: Vec<String>,
    state: ReaderState,
    row_num: usize,
    is_first_row: bool,
    fileobj: Option<Py<PyAny>>,
    size_ref: Arc<AtomicU64>,
    finished: bool,
    current_row: Option<Vec<Py<PyAny>>>,
}


#[pymethods]
impl CsvReaderIterator {
    #[new]
    fn new(
        py: Python<'_>,
        fileobj: &Bound<'_, PyAny>,
        metadata: Option<Vec<HashMap<String, String>>>,
        has_header: Option<bool>,
        delimiter: Option<String>,
        quote_char: Option<String>,
        encoding: Option<String>,
    ) -> PyResult<Self> {
        let mut type_converter = TypeConverter::new();
        type_converter.init_python_objects(py)?;
        let delim = delimiter.unwrap_or_else(|| ",".to_string());
        let quote = quote_char.unwrap_or_else(|| "\"".to_string());
        let enc_name = encoding.unwrap_or_else(|| "utf-8".to_string());
        let encoding = crate::get_encoding(&enc_name);
        let mut metadata_map = HashMap::new();
        let mut column_order = Vec::new();

        if let Some(meta_list) = metadata {
            for item in meta_list {
                for (col_name, col_type) in item {
                    if !column_order.contains(&col_name) {
                        column_order.push(col_name.clone());
                    }
                    metadata_map.insert(col_name, col_type);
                }
            }
        }

        Ok(CsvReaderIterator {
            parser: CsvParser::new(
                delim.as_bytes()[0],
                quote.as_bytes()[0],
                encoding,
            ),
            type_converter,
            metadata: metadata_map,
            column_order,
            has_header: has_header.unwrap_or(true),
            headers: Vec::new(),
            state: ReaderState {
                buffer: Vec::with_capacity(CHUNK_SIZE),
                pos_in_buffer: 0,
                eof: false,
            },
            row_num: 0,
            is_first_row: true,
            fileobj: Some(fileobj.clone().unbind()),
            size_ref: Arc::new(AtomicU64::new(0)),
            finished: false,
            current_row: None,
        })
    }

    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(
        mut slf: PyRefMut<'_, Self>,
        py: Python<'_>,
    ) -> PyResult<Option<Py<PyAny>>> {

        if slf.finished {
            return Ok(None);
        }

        if let Some(row) = slf.current_row.take() {
            let tuple = PyTuple::new(py, row)?;
            return Ok(Some(tuple.unbind().into()));
        }

        let fileobj = match &slf.fileobj {
            Some(f) => f.bind(py),
            None => return Ok(None),
        };

        let mut reader = PyReader::new(fileobj)?;

        loop {
            let mut state = std::mem::take(&mut slf.state);
            let result = slf.parser.read_row_from_buffer(
                &mut reader,
                &mut state,
            );

            match result {
                Ok(Some(row)) => {
                    slf.state = state;

                    if slf.is_first_row && slf.has_header {
                        slf.headers = row;
                        slf.is_first_row = false;
                        continue;
                    }

                    slf.is_first_row = false;
                    slf.row_num += 1;
                    slf.size_ref.store(slf.row_num as u64, Ordering::Relaxed);
                    let converted = slf.convert_row(py, row)?;
                    slf.current_row = Some(converted);
                    let tuple = PyTuple::new(
                        py,
                        slf.current_row.take().unwrap(),
                    )?;
                    return Ok(Some(tuple.unbind().into()));
                }
                Ok(None) => {
                    slf.state = state;
                    slf.finished = true;
                    return Ok(None);
                }
                Err(e) => {
                    slf.state = state;
                    return Err(
                        PyErr::new::<pyo3::exceptions::PyValueError, _>(e),
                    );
                }
            }
        }
    }

    fn tell(&self, py: Python<'_>) -> PyResult<u64> {

        if let Some(fileobj) = &self.fileobj {
            let obj = fileobj.bind(py);
            let pos = obj.call_method0("tell")?;
            let file_pos: u64 = pos.extract()?;
            Ok(file_pos - (
                self.state.buffer.len() - self.state.pos_in_buffer) as u64
            )
        } else {
            Ok(0)
        }
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(fileobj) = &self.fileobj {
            let obj = fileobj.bind(py);
            obj.call_method0("close")?;
            self.fileobj = None;
        }
        Ok(())
    }

    fn row_count(&self) -> usize {
        self.row_num
    }

    fn get_headers(&self) -> Vec<String> {
        self.headers.clone()
    }
}

impl CsvReaderIterator {
    fn convert_row(
        &self,
        py: Python<'_>,
        row: Vec<String>,
    ) -> PyResult<Vec<Py<PyAny>>> {
        let mut result = Vec::with_capacity(row.len());

        for (idx, value) in row.iter().enumerate() {
            let col_name = if self.has_header && idx < self.headers.len() {
                self.headers[idx].clone()
            } else if idx < self.column_order.len() {
                self.column_order[idx].clone()
            } else {
                format!("col_{}", idx)
            };
            let col_type = self.metadata.get(&col_name);
            let py_value = if value.is_empty() {
                py.None()
            } else {
                match col_type {
                    Some(t) => self.type_converter.convert_field(
                        py,
                        value,
                        t,
                    )?,
                    None => value.into_pyobject(py)?.unbind().into(),
                }
            };

            result.push(py_value);
        }

        Ok(result)
    }
}
