import glob
import os
from setuptools import setup, find_packages

install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]

with open("README.md") as fh:
    long_description = fh.read()

def get_version():
    pkg_root = os.path.abspath(os.path.join(os.path.dirname(__file__)))  # noqa
    filepath = os.path.join(pkg_root, "terra_notebook_utils", "version.py")
    version = dict()
    with open(filepath) as fh:
        exec(fh.read().strip(), version)
    return version['__version__']

setup(
    name="terra-notebook-utils",
    version=get_version(),
    description="Utilities for the Terra notebook environment.",
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="https://github.com/DataBiosphere/terra-notebook-utils",
    author="Brian Hannafious",
    author_email="bhannafi@ucsc.edu",
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    entry_points=dict(console_scripts=['tnu=terra_notebook_utils.cli.main:main']),
    zip_safe=False,
    install_requires=install_requires,
    platforms=["MacOS X", "Posix"],
    test_suite="test",
    classifiers=[
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.10"
    ]
)
