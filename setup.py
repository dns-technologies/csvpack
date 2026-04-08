from setuptools import setup, find_packages
from setuptools_rust import RustExtension

setup(
    name="csvpack",
    version="0.1.0.dev5",
    description=(
        "Library for read and write CSV dumps."
    ),
    long_description=open("README.md").read(),
    long_description_content_type="text/markdown",
    author="0xMihalich",
    author_email="bayanmobile87@gmail.com",
    url="https://dns-technologies.github.io/dbhose_airflow/classes/csvpack/index.html",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    rust_extensions=[
        RustExtension(
            "csvpack.csvlib.core",
            path="src/csvpack/csvlib/core/Cargo.toml",
            debug=False,
        )
    ],
    install_requires=[
        "light_compressor==0.1.1.dev2",
        "pandas>=2.1.0",
        "polars>=0.20.31",
    ],
    include_package_data=True,
    zip_safe=False,
    python_requires=">=3.10",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
    ],
)
