#!/usr/bin/env python

from __future__ import with_statement

from setuptools import setup, find_packages

with open("README") as readme:
    documentation = readme.read()

setup(
    name="yatfs",
    version="0.4.0",
    description="FUSE-based torrent-backed filesystem",
    long_description=documentation,
    author="AllSeeingEyeTolledEweSew",
    author_email="allseeingeyetolledewesew@protonmail.com",
    url="http://github.com/AllSeeingEyeTolledEweSew/yatfs",
    license="Unlicense",
    packages=find_packages(),
    use_2to3=True,
    entry_points={
        "console_scripts": [
            "yatfs = yatfs.main:main"
        ]
    },
    install_requires=[
        "deluge-client-sync>=1.0.0",
        "better-bencode>=0.2.1",
        "PyYAML>=3.12",
	"llfuse>=1.3,<2.0",
	"btn>=1.0.0",
    ],
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: End Users/Desktop",
        "License :: Public Domain",
        "Programming Language :: Python",
        "Topic :: Communications :: File Sharing",
        "Topic :: System :: Filesystems",
        "Topic :: System :: Networking",
        "Operating System :: POSIX :: Linux",
        "Operating System :: MacOS :: MacOS X",
        "Operating System :: POSIX :: BSD :: FreeBSD",
        "License :: Public Domain",
    ],
)
