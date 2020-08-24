#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='tap-forecast',
    version='1.0.2',
    description='Singer.io tap for extracting data from the Forecast API',
    author='ngteric',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_forecast'],
    install_requires=[
        'singer-python>=5.9.0',
        'requests>=2.20.0'
    ],
    entry_points='''
        [console_scripts]
        tap-forecast=tap_forecast:main
    ''',
    packages=['tap_forecast'],
    package_data={
        'tap_forecast': ['tap_forecast/*.json']
    },
    include_package_data=True
)
