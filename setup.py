#!/usr/bin/env python
from setuptools import setup

setup(
    name="target-s3boto3",
    version="0.1.0",
    description="Singer.io target for writing data to s3",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["target_s3boto3"],
    install_requires=[
        "boto3>=1.4.1",
        "singer-python>=5.0.12",
    ],
    entry_points="""
    [console_scripts]
    target-s3boto3=target_s3boto3:main
    """,
    packages=["target_s3boto3"],
    package_data = {},
    include_package_data=True,
)
