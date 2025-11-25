import os
from distutils.core import Extension

from setuptools import find_packages, setup

def src_local(rel_path):
    return os.path.abspath(os.path.join(os.path.dirname(os.path.realpath(__file__)), rel_path))

def get_version():
    with open(src_local("VERSION"), "r") as f:
        version = f.readline().strip()
    return version

def get_description():
    with open("README.md", "r") as README:
        description = README.read()

data_files = [(".", ["LICENSE", "README.md"])]

setup(
    name="async_dsmz",
    # packages=find_packages(where="src"),
    url="https://github.com/Fabian-Bastiaanssen/Async_DSMZ",
    python_requires=">=3.14.0",
    description="async_dsmz: async fetching of DSMZ data",
    long_description=get_description(),
    long_description_content_type="text/markdown",
    version=get_version(),
    author="Fabian Bastiaanssen",
    author_email="Fbastiaanssen@umail.ucc.ie",
    data_files=data_files,
    py_modules=["async_dsmz"],
    install_requires=[
        "aiohttp>=3.13.2",
        "bacdive>=1.0.0",
        "lpsn>=1.0.0",
        "setuptools<=80",
    ],
    include_package_data=True,
    classifiers=[
        "Development Status :: 5 - Production/Stable",
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GPL-3.0-only",
        "Natural Language :: English",
        "Topic :: Scientific/Engineering :: Bio-Informatics",
        "Operating System :: OS Independent",
    ],
)
