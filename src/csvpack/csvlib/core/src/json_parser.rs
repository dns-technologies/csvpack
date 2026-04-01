use pyo3::prelude::*;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyDict, PyList, PyBytes};
use pyo3::IntoPyObjectExt;
use serde_json::Value;


fn decode_hex_bytes(value: &str) -> PyResult<Vec<u8>> {
    let hex_str = if value.starts_with("\\x") {
        &value[2..]
    } else if value.starts_with("0x") {
        &value[2..]
    } else {
        return Err(PyValueError::new_err(
            format!("Invalid hex prefix: {}", value)
        ));
    };

    if hex_str.len() % 2 != 0 {
        return Err(PyValueError::new_err(
            format!("Odd-length hex string: {}", hex_str)
        ));
    }

    let mut bytes = Vec::with_capacity(hex_str.len() / 2);
    let mut chars = hex_str.chars();

    while let Some(c1) = chars.next() {
        let c2 = chars.next().ok_or_else(|| {
            PyValueError::new_err(format!("Invalid hex string: {}", hex_str))
        })?;
        
        let byte = u8::from_str_radix(&format!("{}{}", c1, c2), 16)
            .map_err(|e| PyValueError::new_err(
                format!("Invalid hex digit: {}", e)
            ))?;
        bytes.push(byte);
    }

    Ok(bytes)
}


fn decode_escape_bytes(value: &str) -> PyResult<Vec<u8>> {
    let mut bytes = Vec::new();
    let mut chars = value.chars().peekable();

    while let Some(c) = chars.next() {
        if c == '\\' {
            let next = chars.next().ok_or_else(|| {
                PyValueError::new_err(
                    "Invalid escape sequence at end of string"
                )
            })?;

            if next == '\\' {
                bytes.push(b'\\');
            } else if next.is_ascii_digit() {
                let mut octal = next.to_string();
                for _ in 0..2 {
                    if let Some(d) = chars.next() {
                        octal.push(d);
                    } else {
                        break;
                    }
                }

                let byte = u8::from_str_radix(&octal, 8)
                    .map_err(|e| PyValueError::new_err(
                        format!("Invalid octal: {}", e)
                    ))?;
                bytes.push(byte);
            } else {
                bytes.push(next as u8);
            }
        } else {
            bytes.push(c as u8);
        }
    }

    Ok(bytes)
}


pub fn parse_binary_to_python(
    py: Python<'_>,
    value: &str,
) -> PyResult<Py<PyAny>> {
    let bytes = if value.starts_with("\\x") || value.starts_with("0x") {
        decode_hex_bytes(value)?
    } else if value.contains('\\') {
        decode_escape_bytes(value)?
    } else {
        return Ok(value.as_bytes().into_pyobject(py)?.unbind().into());
    };

    Ok(PyBytes::new(py, &bytes).unbind().into())
}


fn parse_python_literal(value: &str) -> Result<Value, String> {
    let mut json_str = value.to_string();
    let mut in_string = false;
    let mut in_escape = false;
    let mut result = String::with_capacity(json_str.len());
    let chars: Vec<char> = json_str.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        let c = chars[i];

        if in_escape {
            result.push(c);
            in_escape = false;
            i += 1;
            continue;
        }

        if c == '\\' {
            in_escape = true;
            result.push(c);
            i += 1;
            continue;
        }

        if c == '\'' && !in_string {
            in_string = true;
            result.push('"');
            i += 1;
            continue;
        }

        if c == '\'' && in_string {
            in_string = false;
            result.push('"');
            i += 1;
            continue;
        }

        result.push(c);
        i += 1;
    }

    json_str = result;

    if json_str.starts_with('{') && json_str.ends_with('}') {
        if !json_str.contains(':') {
            let inner = &json_str[1..json_str.len()-1];
            json_str = format!("[{}]", inner);
        }
    }

    serde_json::from_str(&json_str)
        .map_err(
            |e| format!(
                "Failed to parse Python literal: {} (converted: {})",
                e,
                json_str
            )
        )
}


pub fn parse_json_to_python(
    py: Python<'_>,
    value: &str,
) -> PyResult<Py<PyAny>> {
    let json_value = match parse_python_literal(value) {
        Ok(v) => v,
        Err(_) => {
            let mut value_copy = value.to_string();
            match unsafe { simd_json::from_str(&mut value_copy) } {
                Ok(v) => v,
                Err(_) => {
                    match serde_json::from_str::<Value>(value) {
                        Ok(v) => v,
                        Err(_) => {
                            return Ok(value.into_py_any(py)?);
                        }
                    }
                }
            }
        }
    };

    serde_value_to_python(py, &json_value)
}


fn serde_value_to_python(
    py: Python<'_>,
    value: &Value,
) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok(b.into_py_any(py)?),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_py_any(py)?)
            } else if let Some(f) = n.as_f64() {
                Ok(f.into_py_any(py)?)
            } else {
                Ok(n.to_string().into_py_any(py)?)
            }
        }
        Value::String(s) => Ok(s.clone().into_py_any(py)?),
        Value::Array(arr) => {
            let py_list = PyList::empty(py);
            for item in arr {
                let converted = serde_value_to_python(py, item)?;
                py_list.append(converted)?;
            }
            Ok(py_list.unbind().into())
        }
        Value::Object(obj) => {
            let py_dict = PyDict::new(py);
            for (k, v) in obj {
                let converted = serde_value_to_python(py, v)?;
                py_dict.set_item(k.as_str(), converted)?;
            }
            Ok(py_dict.unbind().into())
        }
    }
}
