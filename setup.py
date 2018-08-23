#!/usr/bin/env python
"""Install this package. Requires sdss3tools.

To use:
python setup.py install
"""
import sdss3tools


sdss3tools.setup(
    description='Code base for SDSS-III SOP actor',
    data_dirs=['scripts'],
)
