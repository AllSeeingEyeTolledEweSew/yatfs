#!/usr/bin/env python

from __future__ import with_statement

from setuptools import setup, find_packages

with open("README") as readme:
    documentation = readme.read()

setup(
    name="yatfs",
    version="0.1.0",
    description="FUSE-based torrent-backed filesystem",
    long_description=documentation,
    author="AllSeeingEyeTolledEweSew",
    author_email="allseeingeyetolledewesew@protonmail.com",
    url="http://github.com/AllSeeingEyeTolledEweSew/yatfs",
    license="Unlicense",
    packages=find_packages(),
    entry_points={
        "console_scripts": [
            "yatfs = yatfs.main:main"
        ]
    },
    python_requires=">=3.4",
    install_requires=[
        "deluge-client-async>=0.1.0",
        "better-bencode>=0.2.1",
        "PyYAML>=3.12",
    ],
)
