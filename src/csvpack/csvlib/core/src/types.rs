use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use crate::json_parser::{parse_binary_to_python, parse_json_to_python};
use chrono::{NaiveDate, NaiveDateTime, NaiveTime, Datelike, Timelike};


pub struct TypeConverter {
    datetime_module: Option<Py<PyAny>>,
    date_class: Option<Py<PyAny>>,
    datetime_class: Option<Py<PyAny>>,
    time_class: Option<Py<PyAny>>,
    uuid_module: Option<Py<PyAny>>,
}


impl TypeConverter {
    pub fn new() -> Self {
        TypeConverter {
            datetime_module: None,
            date_class: None,
            datetime_class: None,
            time_class: None,
            uuid_module: None,
        }
    }

    pub fn init_python_objects(&mut self, py: Python<'_>) -> PyResult<()> {
        let datetime = py.import("datetime")?;
        self.datetime_module = Some(datetime.clone().unbind().into());
        self.date_class = Some(datetime.getattr("date")?.unbind().into());
        self.datetime_class = {
            Some(datetime.getattr("datetime")?.unbind().into())
        };
        self.time_class = Some(datetime.getattr("time")?.unbind().into());
        let uuid = py.import("uuid")?;
        self.uuid_module = Some(uuid.unbind().into());
        Ok(())
    }

    pub fn convert_field(
        &self,
        py: Python<'_>,
        value: &str,
        type_name: &str,
    ) -> PyResult<Py<PyAny>> {
        if value.is_empty() {
            return Ok(py.None());
        }

        let (base_type, inner_type) = self.parse_complex_type(type_name);

        match base_type {
            "list" => {
                let parsed = self.parse_json_to_value(py, value)?;

                if let Some(inner) = inner_type {
                    self.convert_list_elements(py, parsed, inner)
                } else {
                    Ok(parsed)
                }

            }
            "int" => {
                match value.parse::<i64>() {
                    Ok(v) => Ok(v.into_pyobject(py)?.unbind().into()),
                    Err(_) => Ok(py.None()),
                }
            }
            "float" => {
                match value.parse::<f64>() {
                    Ok(v) => Ok(v.into_pyobject(py)?.unbind().into()),
                    Err(_) => Ok(py.None()),
                }
            }
            "bool" => {
                match value.to_lowercase().as_str() {
                    "true" | "1" | "yes" => Ok(true.into_py_any(py)?),
                    "false" | "0" | "no" => Ok(false.into_py_any(py)?),
                    _ => Ok(py.None()),
                }
            }
            "str" => {
                Ok(value.into_pyobject(py)?.unbind().into())
            }
            "bytes" => {
                parse_binary_to_python(py, value)
            }
            "date" => self.parse_date_to_python(py, value),
            "datetime" => self.parse_datetime_to_python(py, value),
            "time" => self.parse_time_to_python(py, value),
            "uuid" => self.parse_uuid_to_python(py, value),
            "json" => {
                parse_json_to_python(py, value)
            }
            _ => {
                Ok(value.into_pyobject(py)?.unbind().into())
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

    fn parse_json_to_value(
        &self,
        py: Python<'_>,
        value: &str,
    ) -> PyResult<Py<PyAny>> {
        parse_json_to_python(py, value)
    }

    fn convert_list_elements(
        &self,
        py: Python<'_>,
        list_obj: Py<PyAny>,
        inner_type: &str,
    ) -> PyResult<Py<PyAny>> {
        let bound = list_obj.bind(py);

        if !bound.is_instance_of::<pyo3::types::PyList>() {
            return Ok(list_obj);
        }

        let py_list = bound.cast::<pyo3::types::PyList>()?;
        let mut result = Vec::with_capacity(py_list.len());
        let is_numeric_type = inner_type == "int" || inner_type == "float";

        for item in py_list.iter() {
            if is_numeric_type &&
                (item.is_instance_of::<pyo3::types::PyInt>() ||
                item.is_instance_of::<pyo3::types::PyFloat>()) {
                let num_str = item.str()?.to_string();
                let converted = self.convert_field(
                    py,
                    &num_str,
                    inner_type,
                )?;
                result.push(converted);
                continue;
            }

            if item.is_instance_of::<pyo3::types::PyString>() {
                let item_str: String = item.extract()?;
                let converted = self.convert_field(
                    py,
                    &item_str,
                    inner_type,
                )?;
                result.push(converted);
                continue;
            }

            result.push(item.into_pyobject(py)?.unbind().into());
        }

        Ok(result.into_pyobject(py)?.unbind().into())
    }

    fn parse_date_to_python(
        &self,
        py: Python<'_>,
        value: &str,
    ) -> PyResult<Py<PyAny>> {
        let formats = [
            "%Y-%m-%d",
            "%Y/%m/%d",
            "%d/%m/%Y",
            "%m/%d/%Y",
            "%d-%m-%Y",
            "%m-%d-%Y",
        ];

        for fmt in &formats {
            if let Ok(date) = NaiveDate::parse_from_str(value, fmt) {
                if let Some(date_class) = &self.date_class {
                    let date_class_bound = date_class.bind(py);
                    let result = date_class_bound.call_method1(
                        "__new__",
                        (
                            date_class_bound.clone(),
                            date.year(),
                            date.month(),
                            date.day(),
                        )
                    )?;
                    return Ok(result.unbind().into());
                }
            }
        }

        Ok(py.None())
    }

    fn parse_datetime_to_python(
        &self,
        py: Python<'_>,
        value: &str,
    ) -> PyResult<Py<PyAny>> {
        let processed_value = if let Some(dot_pos) = value.find('.') {
            let after_dot = &value[dot_pos + 1..];
            if after_dot.chars().all(|c| c.is_ascii_digit()) {
                if after_dot.len() > 6 {
                    let trimmed = &after_dot[..6];
                    format!("{}.{}", &value[..dot_pos], trimmed)
                } else {
                    value.to_string()
                }
            } else {
                value.to_string()
            }
        } else {
            value.to_string()
        };

        let formats_with_micros = [
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%dT%H:%M:%S.%f",
        ];

        for fmt in &formats_with_micros {
            if let Ok(dt) = NaiveDateTime::parse_from_str(
                &processed_value,
                fmt,
            ) {
                if let Some(datetime_class) = &self.datetime_class {
                    let datetime_class_bound = datetime_class.bind(py);
                    let micros = dt.and_utc().timestamp_subsec_micros();
                    let result = datetime_class_bound.call_method1(
                        "__new__",
                        (
                            datetime_class_bound.clone(),
                            dt.year(),
                            dt.month(),
                            dt.day(),
                            dt.hour(),
                            dt.minute(),
                            dt.second(),
                            micros,
                        )
                    )?;
                    return Ok(result.unbind().into());
                }
            }
        }

        let formats_without_micros = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y/%m/%d %H:%M:%S",
            "%d/%m/%Y %H:%M:%S",
        ];

        for fmt in &formats_without_micros {
            if let Ok(dt) = NaiveDateTime::parse_from_str(
                &processed_value,
                fmt,
            ) {
                if let Some(datetime_class) = &self.datetime_class {
                    let datetime_class_bound = datetime_class.bind(py);
                    let result = datetime_class_bound.call_method1(
                        "__new__",
                        (
                            datetime_class_bound.clone(),
                            dt.year(),
                            dt.month(),
                            dt.day(),
                            dt.hour(),
                            dt.minute(),
                            dt.second(),
                        )
                    )?;
                    return Ok(result.unbind().into());
                }
            }
        }

        Ok(py.None())
    }

    fn parse_time_to_python(
        &self,
        py: Python<'_>,
        value: &str,
    ) -> PyResult<Py<PyAny>> {
        let processed_value = if let Some(dot_pos) = value.find('.') {
            let after_dot = &value[dot_pos + 1..];
            if after_dot.chars().all(|c| c.is_ascii_digit()) {
                if after_dot.len() > 6 {
                    let trimmed = &after_dot[..6];
                    format!("{}.{}", &value[..dot_pos], trimmed)
                } else {
                    value.to_string()
                }
            } else {
                value.to_string()
            }
        } else {
            value.to_string()
        };

        let formats_with_micros = [
            "%H:%M:%S.%f",
            "%I:%M:%S.%f %p",
        ];

        for fmt in &formats_with_micros {
            if let Ok(time) = NaiveTime::parse_from_str(
                &processed_value,
                fmt,
            ) {
                if let Some(time_class) = &self.time_class {
                    let time_class_bound = time_class.bind(py);
                    let micros = time.nanosecond() / 1000;
                    let result = time_class_bound.call_method1(
                        "__new__",
                        (
                            time_class_bound.clone(),
                            time.hour(),
                            time.minute(),
                            time.second(),
                            micros,
                        )
                    )?;
                    return Ok(result.unbind().into());
                }
            }
        }

        let formats_without_micros = [
            "%H:%M:%S",
            "%H:%M",
            "%I:%M:%S %p",
            "%I:%M %p",
        ];

        for fmt in &formats_without_micros {
            if let Ok(time) = NaiveTime::parse_from_str(
                &processed_value,
                fmt,
            ) {
                if let Some(time_class) = &self.time_class {
                    let time_class_bound = time_class.bind(py);
                    let result = time_class_bound.call_method1(
                        "__new__",
                        (
                            time_class_bound.clone(),
                            time.hour(),
                            time.minute(),
                            time.second(),
                            0,
                        )
                    )?;
                    return Ok(result.unbind().into());
                }
            }
        }

        Ok(value.into_pyobject(py)?.unbind().into())
    }

    fn parse_uuid_to_python(
        &self,
        py: Python<'_>,
        value: &str,
    ) -> PyResult<Py<PyAny>> {
        if let Some(uuid_module) = &self.uuid_module {
            let uuid_class_bound = uuid_module.bind(py);
            match uuid_class_bound.call_method1("UUID", (value,)) {
                Ok(uuid) => return Ok(uuid.unbind().into()),
                Err(_) => {}
            }
        }
        Ok(value.into_pyobject(py)?.unbind().into())
    }
}
