use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::exceptions::PyValueError;
use std::io::Read;
use std::collections::HashMap;
use encoding_rs::{Encoding, UTF_8, WINDOWS_1251, WINDOWS_1252};

mod constants;
mod parser;
mod types;
mod json_parser;

use constants::BUFFER_SIZE;
use parser::CsvParser;
use types::TypeConverter;


fn is_nan(py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<bool> {
    if let Ok(f) = value.extract::<f64>() {
        return Ok(f.is_nan());
    }

    let math = py.import("math")?;
    let isnan = math.getattr("isnan")?;

    match isnan.call1((value,)) {
        Ok(result) => Ok(result.extract::<bool>()?),
        Err(_) => Ok(false),
    }
}


fn get_encoding(name: &str) -> &'static Encoding {
    match name.to_lowercase().as_str() {
        "utf-8" | "utf8" => UTF_8,
        "windows-1251" | "cp1251" => WINDOWS_1251,
        "windows-1252" | "cp1252" => WINDOWS_1252,
        "iso-8859-1" | "latin1" => WINDOWS_1252,
        _ => UTF_8,
    }
}


fn serialize_bytes_to_hex(bytes: &[u8]) -> PyResult<String> {
    let hex_str: String = bytes.iter()
        .map(|b| format!("{:02x}", b))
        .collect();
    Ok(format!("\\x{}", hex_str))
}


fn serialize_to_csv(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<String> {
    if value.is_none() {
        return Ok(String::new());
    }

    if let Ok(i) = value.extract::<i64>() {
        return Ok(i.to_string());
    }

    if let Ok(f) = value.extract::<f64>() {
        if f.fract() == 0.0 {
            return Ok((f as i64).to_string());
        }
        return Ok(f.to_string());
    }

    if let Ok(b) = value.extract::<bool>() {
        return Ok(b.to_string().to_lowercase());
    }

    if let Ok(py_bytes) = value.cast::<pyo3::types::PyBytes>() {
        let bytes = py_bytes.as_bytes();
        return serialize_bytes_to_hex(bytes);
    }

    if let Ok(uuid) = value.call_method0("__str__") {
        let s: String = uuid.extract()?;
        if s.len() == 36 && s.chars().filter(|&c| c == '-').count() == 4 {
            return Ok(s);
        }
    }

    if value.is_instance_of::<pyo3::types::PyList>() {
        let py_list = value.cast::<pyo3::types::PyList>()?;
        let mut elements = Vec::new();

        for item in py_list.iter() {
            elements.push(serialize_list_element(py, &item)?);
        }

        return Ok(format!("[{}]", elements.join(",")));
    }

    if value.is_instance_of::<pyo3::types::PyDict>() {
        let py_dict = value.cast::<pyo3::types::PyDict>()?;
        let mut items = Vec::new();

        for (key, val) in py_dict.iter() {
            let key_str: String = key.extract()?;
            let key_escaped = key_str.replace("'", "\\'");
            let val_str = serialize_to_csv(py, &val)?;
            items.push(format!("'{}':{}", key_escaped, val_str));
        }

        return Ok(format!("{{{}}}", items.join(",")));
    }

    let str_repr = value.call_method0("__str__")?;
    Ok(str_repr.extract()?)
}


fn serialize_list_element(
    py: Python<'_>,
    value: &Bound<'_, PyAny>,
) -> PyResult<String> {
    if let Ok(i) = value.extract::<i64>() {
        return Ok(i.to_string());
    }

    if let Ok(f) = value.extract::<f64>() {
        if f.fract() == 0.0 {
            return Ok((f as i64).to_string());
        }
        return Ok(f.to_string());
    }

    if let Ok(b) = value.extract::<bool>() {
        return Ok(b.to_string().to_lowercase());
    }

    if value.is_instance_of::<pyo3::types::PyList>() {
        let py_list = value.cast::<pyo3::types::PyList>()?;
        let mut elements = Vec::new();

        for item in py_list.iter() {
            elements.push(serialize_list_element(py, &item)?);
        }

        return Ok(format!("[{}]", elements.join(",")));
    }

    if value.is_instance_of::<pyo3::types::PyDict>() {
        let py_dict = value.cast::<pyo3::types::PyDict>()?;
        let mut items = Vec::new();

        for (key, val) in py_dict.iter() {
            let key_str: String = key.extract()?;
            let key_escaped = key_str.replace("'", "\\'");
            let val_str = serialize_list_element(py, &val)?;
            items.push(format!("'{}':{}", key_escaped, val_str));
        }

        return Ok(format!("{{{}}}", items.join(",")));
    }

    let str_repr = value.call_method0("__str__")?;
    let s: String = str_repr.extract()?;
    let escaped = s.replace("'", "\\'");
    Ok(format!("'{}'", escaped))
}


#[pyclass]
pub struct RustCsvReader {
    parser: CsvParser,
    type_converter: TypeConverter,
    metadata: HashMap<String, String>,
    column_order: Vec<String>,
    has_header: bool,
    headers: Vec<String>,
    reader: Option<PyReader>,
    buffer: Vec<u8>,
    pos_in_buffer: usize,
    row_num: usize,
    is_first_row: bool,
    eof: bool,
}


#[pymethods]
impl RustCsvReader {
    #[new]
    fn new(
        fileobj: &Bound<'_, PyAny>,
        metadata: Option<Vec<HashMap<String, String>>>,
        has_header: Option<bool>,
        delimiter: Option<String>,
        quote_char: Option<String>,
        encoding: Option<String>,
    ) -> PyResult<Self> {
        let mut converter = TypeConverter::new();

        Python::attach(|py| -> PyResult<()> {
            converter.init_python_objects(py)?;
            Ok(())
        })?;

        let delim = delimiter.unwrap_or_else(|| ",".to_string());
        let quote = quote_char.unwrap_or_else(|| "\"".to_string());
        let enc_name = encoding.unwrap_or_else(|| "utf-8".to_string());
        let encoding = get_encoding(&enc_name);
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

        Ok(RustCsvReader {
            parser: CsvParser::new(
                delim.as_bytes()[0],
                quote.as_bytes()[0],
                encoding,
            ),
            type_converter: converter,
            metadata: metadata_map,
            column_order,
            has_header: has_header.unwrap_or(true),
            headers: Vec::new(),
            reader: Some(PyReader::new(fileobj)?),
            buffer: Vec::with_capacity(BUFFER_SIZE),
            pos_in_buffer: 0,
            row_num: 0,
            is_first_row: true,
            eof: false,
        })
    }

    fn __next__(&mut self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        if self.eof {
            return Ok(None);
        }

        let reader = match &mut self.reader {
            Some(r) => r,
            None => return Ok(None),
        };

        loop {
            match self.parser.read_row_from_buffer(
                reader,
                &mut self.buffer,
                &mut self.pos_in_buffer,
                &mut self.eof,
            ) {
                Ok(Some(row)) => {
                    if self.is_first_row && self.has_header {
                        self.headers = row;
                        self.is_first_row = false;
                        continue;
                    }

                    self.is_first_row = false;
                    self.row_num += 1;
                    let converted = self.convert_row(py, row)?;
                    return Ok(Some(converted));
                }
                Ok(None) => {
                    self.eof = true;
                    return Ok(None);
                }
                Err(e) => return Err(PyValueError::new_err(e)),
            }
        }
    }

    fn __iter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    fn convert_row(
        &self,
        py: Python<'_>,
        row: Vec<String>,
    ) -> PyResult<Py<PyAny>> {
        let mut tuple = Vec::new();

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

            tuple.push(py_value);
        }

        Ok(tuple.into_pyobject(py)?.unbind().into())
    }

    fn tell(&self) -> PyResult<u64> {
        Python::attach(|py| {
            if let Some(reader) = &self.reader {
                let file_pos = reader.tell(py)?;
                Ok(file_pos - (self.buffer.len() - self.pos_in_buffer) as u64)
            } else {
                Ok(0)
            }
        })
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        if let Some(reader) = &mut self.reader {
            reader.close(py)?;
            self.reader = None;
        }
        Ok(())
    }

    fn get_headers(&self) -> Vec<String> {
        self.headers.clone()
    }

    fn row_count(&self) -> usize {
        self.row_num
    }
}


#[pyclass]
pub struct RustCsvWriter {
    delimiter: u8,
    quote_char: u8,
    line_terminator: String,
    column_order: Vec<String>,
    has_header: bool,
    headers_written: bool,
    pos: u64,
    encoding: &'static Encoding,
    _type_converter: TypeConverter,
}


impl RustCsvWriter {

    fn serialize_field(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        type_name: &str,
    ) -> PyResult<String> {
        if value.is_none() {
            return Ok(String::new());
        }

        let (base_type, _inner_type) = self.parse_complex_type(type_name);

        match base_type {
            "list" => {
                if !value.is_instance_of::<pyo3::types::PyList>() {
                    return serialize_to_csv(py, value);
                }
                
                let py_list = value.cast::<pyo3::types::PyList>()?;
                let mut elements = Vec::new();
                
                for item in py_list.iter() {
                    elements.push(serialize_list_element(py, &item)?);
                }
                
                Ok(format!("[{}]", elements.join(",")))
            }
            _ => {
                serialize_to_csv(py, value)
            }
        }
    }

    fn parse_complex_type<'a>(
        &self,
        type_name: &'a str,
    ) -> (&'a str, Option<&'a str>) {
        if let Some(open_bracket) = type_name.find('[') {
            if let Some(close_bracket) = type_name.find(']') {
                let base = &type_name[..open_bracket];
                let inner = &type_name[open_bracket + 1..close_bracket];
                return (base, Some(inner));
            }
        }
        (type_name, None)
    }

    fn value_to_string(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<String> {
        if value.is_none() || is_nan(py, value)? {
            return Ok(String::new());
        }

        serialize_to_csv(py, value)
    }

    fn write_escaped_field(&self, field: &str) -> Vec<u8> {
        let needs_quoting = field.contains(self.delimiter as char)
            || field.contains(self.quote_char as char)
            || field.contains('\n')
            || field.contains('\r');
        let mut result = Vec::new();

        if needs_quoting {
            result.push(self.quote_char);

            for c in field.chars() {
                if c == self.quote_char as char {
                    result.push(self.quote_char);
                }
                result.extend_from_slice(c.to_string().as_bytes());
            }

            result.push(self.quote_char);
        } else {
            result.extend_from_slice(field.as_bytes());
        }

        result
    }

    fn write_row_bytes(
        &mut self,
        py: Python<'_>,
        row: &Bound<'_, PyAny>,
        metadata: &HashMap<String, String>,
    ) -> PyResult<Vec<u8>> {
        let len = row.len()?;
        let mut row_buffer = Vec::new();

        if !self.headers_written &&
            self.has_header &&
            !self.column_order.is_empty() {
            let mut i = 0;
            for col_name in &self.column_order {
                if i > 0 {
                    row_buffer.push(self.delimiter);
                }
                let (encoded, _, _) = self.encoding.encode(col_name);
                row_buffer.extend_from_slice(&encoded);
                i += 1;
            }
            row_buffer.extend_from_slice(self.line_terminator.as_bytes());
            self.headers_written = true;
        }

        for i in 0..len {
            if i > 0 {
                row_buffer.push(self.delimiter);
            }

            let item = row.get_item(i)?;
            let col_type = if i < self.column_order.len() {
                metadata.get(&self.column_order[i])
            } else {
                None
            };
            let item_str = if is_nan(py, &item)? {
                String::new()
            } else if item.is_none() {
                String::new()
            } else {
                match col_type {
                    Some(t) => self.serialize_field(py, &item, t)?,
                    None => self.value_to_string(py, &item)?,
                }
            };
            row_buffer.extend(self.write_escaped_field(&item_str));
        }

        row_buffer.extend_from_slice(self.line_terminator.as_bytes());
        self.pos += row_buffer.len() as u64;
        Ok(row_buffer)
    }
}


#[pymethods]
impl RustCsvWriter {
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
        let encoding = get_encoding(&enc_name);
        let mut column_order = Vec::new();
        let mut metadata_map = HashMap::new();

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

        let mut type_converter = TypeConverter::new();
        Python::attach(|py| -> PyResult<()> {
            type_converter.init_python_objects(py)?;
            Ok(())
        })?;

        Ok(RustCsvWriter {
            delimiter: delim.as_bytes()[0],
            quote_char: quote.as_bytes()[0],
            line_terminator: "\n".to_string(),
            column_order,
            has_header: has_header.unwrap_or(true),
            headers_written: false,
            pos: 0,
            encoding,
            _type_converter: type_converter,
        })
    }

    fn write_row(
        &mut self,
        py: Python<'_>,
        row: &Bound<'_, PyAny>,
        metadata: Option<Vec<HashMap<String, String>>>,
    ) -> PyResult<Py<PyBytes>> {
        let mut metadata_map = HashMap::new();

        if let Some(meta_list) = metadata {
            for item in meta_list {
                for (col_name, col_type) in item {
                    metadata_map.insert(col_name, col_type);
                }
            }
        }

        let bytes = self.write_row_bytes(py, row, &metadata_map)?;
        Ok(PyBytes::new(py, &bytes).unbind())
    }

    fn tell(&self) -> u64 {
        self.pos
    }
}


struct PyReader {
    obj: Py<PyAny>,
    pos: u64,
}


impl PyReader {
    fn new(obj: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(PyReader {
            obj: obj.clone().unbind(),
            pos: 0,
        })
    }

    fn tell(&self, py: Python<'_>) -> PyResult<u64> {
        let obj = self.obj.bind(py);
        let pos = obj.call_method0("tell")?;
        Ok(pos.extract()?)
    }

    fn close(&mut self, py: Python<'_>) -> PyResult<()> {
        let obj = self.obj.bind(py);
        obj.call_method0("close")?;
        Ok(())
    }
}


impl Read for PyReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        Python::attach(|py| {
            let obj = self.obj.bind(py);
            let bytes_obj = match obj.call_method1("read", (buf.len(),)) {
                Ok(b) => b,
                Err(e) => return Err(
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("{:?}", e),
                    )
                ),
            };

            if bytes_obj.is_none() {
                return Ok(0);
            }

            let py_bytes = match bytes_obj.cast::<PyBytes>() {
                Ok(b) => b,
                Err(e) => return Err(
                    std::io::Error::new(
                        std::io::ErrorKind::Other,
                        format!("{:?}", e),
                    )
                ),
            };
            let data = py_bytes.as_bytes();
            let to_copy = buf.len().min(data.len());

            buf[..to_copy].copy_from_slice(&data[..to_copy]);
            self.pos += to_copy as u64;
            Ok(to_copy)
        })
    }
}


#[pymodule]
fn csvcore(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustCsvReader>()?;
    m.add_class::<RustCsvWriter>()?;
    Ok(())
}
