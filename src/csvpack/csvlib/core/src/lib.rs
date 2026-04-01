use pyo3::prelude::*;
use pyo3::types::PyBytes;
use std::io::Read;
use encoding_rs::{Encoding, UTF_8, WINDOWS_1251, WINDOWS_1252};

mod constants;
mod parser;
mod types;
mod json_parser;
mod reader;
mod writer;

use reader::RustCsvReader;
use writer::RustCsvWriter;


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
fn core(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<RustCsvReader>()?;
    m.add_class::<RustCsvWriter>()?;
    Ok(())
}
