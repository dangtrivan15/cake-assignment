#!/usr/bin/env python
from setuptools import setup

requires = [
    "apache-airflow==2.9.0",
]
dev_requires = [
    # "pytest==6.2.0",
    # "testcontainers==3.7.1",
]

setup(
    name="cake-assignment-custom-module",
    version="0.0.1",
    author="Van Dang",
    author_email="dangtrivan15@gmail.com",
    description="",
    long_description="",
    long_description_content_type="text/markdown",
    url="https://github.com/dangtrivan15/cake-assignment",
    packages=["cake_airflow_custom_package"],
    include_package_data=True,
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    tests_require=dev_requires,
    install_requires=requires,
    extras_require={"dev": dev_requires},
)
