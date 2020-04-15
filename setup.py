import glob
import os
from setuptools import setup, find_packages

install_requires = [line.rstrip() for line in open(os.path.join(os.path.dirname(__file__), "requirements.txt"))]

setup(
    name="terra-notebook-utils",
    version="0.0.0",
    description="Utilities for the Terra notebook environment.",
    url="https://github.com/DataBiosphere/terra-notebook-utils",
    author="Brian Hannafious",
    author_email="bhannafi@ucsc.edu",
    license="MIT",
    packages=find_packages(exclude=["tests"]),
    scripts=glob.glob("scripts/*"),
    zip_safe=False,
    install_requires=install_requires,
    platforms=["MacOS X", "Posix"],
    test_suite="test",
    classifiers=[
        "Intended Audience :: Terra notebook developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.7"
    ]
)
