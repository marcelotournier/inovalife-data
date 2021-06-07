#!/usr/bin/env python

from distutils.core import setup

with open("requirements.txt") as file:
    REQUIREMENTS = [line for line in file if (line != "") | (~line.startswith("#"))]

setup(
    name='inovalife_data',
    version='1.0',
    description='Spark Datasus Client'
    author='Marcelo Tournier',
    author_email='marcelo@inova.life',
    url='https://github.com/marcelotournier/inovalife-data',
    packages=['inovalife_data'],
    install_requires=REQUIREMENTS
    )
