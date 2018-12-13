#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='cacheer',
    version='0.1.0',
    packages=find_packages(include=["cacheer"]),
    author='notmeor',
    author_email='kevin.inova@gmail.com',
    description='',
    install_requires=[
        'lmdb=0.94'
    ]
)
