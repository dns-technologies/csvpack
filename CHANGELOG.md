# Version History

## 0.1.0.dev4

* Developer release (not public to pip)
* Add RustCsvWriter.row_count()
* Add CSVWriter.num_rows property method
* Fix CSVWriter.`__repr__`() method
* Swap CSVPackWriter metadata and fileobj initialization parameters
* CSVWriter move fileobj parameter initialization parameters into tail
* Update RustCsvWriter.pyi

## 0.1.0.dev3

* Developer release (not public to pip)
* Change BUFFER_SIZE: usize = 8192 to CHUNK_SIZE: usize = 65536
* Remove Sizes.BUFFER_SIZE constant
* Remove CSVWriter.chunk_size parameter
* Refactor rust code
* Refactor CSVReader and CSVWriter
* Rename csvcore -> core
* Speed-up csv read and write operations
* Improve pytests for new code
* Decomposite CSVPackWriter.from_bytes() method

## 0.1.0.dev2

* Developer release (not public to pip)
* Fix CSVPackWriter.`__init__`() without parameters
* Fix CSVPackWriter.from_bytes() method
* Add pandas.Series, polars.Series and polars.List into dtype.py
* Add is_nan() function into csvcore for RustCsvReader and RustCsvWriter

## 0.1.0.dev1

* Developer release (not public to pip)
* Update depends light_compressor==0.1.1.dev1
* Improve docstrings

## 0.1.0.dev0

* Developer release (not public to pip)
* Improve tests for CSVPack
* Update README.md
* Change pyo3 revision to 0.28.2
* Refactor deprecated Rust code
* Refactor CSVPackMeta class
* Improve csvpack_repr() function
* Refactor CSVPackReader.`__repr__`() method
* Refactor CSVPackWriter.`__repr__`() method
* Update depends light-compressor==0.1.1.dev0

## 0.0.0.1

First version of the library (No public to pip)
