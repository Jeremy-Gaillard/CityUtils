# -*- coding: utf-8 -*-
import os
import re
from setuptools import setup, find_packages


here = os.path.abspath(os.path.dirname(__file__))


requirements = (
    'psycopg2',
    'pyyaml'
)

def find_version(*file_paths):
    """
    see https://github.com/pypa/sampleproject/blob/master/setup.py
    """

    with open(os.path.join(here, *file_paths), 'r') as f:
        version_file = f.read()

    # The version line must have the form
    # __version__ = 'ver'
    version_match = re.search(r"^__version__ = ['\"]([^'\"]*)['\"]",
                              version_file, re.M)
    if version_match:
        return version_match.group(1)
    raise RuntimeError("Unable to find version string. "
                       "Should be at the first line of __init__.py.")


setup(
    name='city_utils',
    version=find_version('city_utils', '__init__.py'),
    description="Various city processing utilities",
    url='https://github.com/Oslandia/TODO',
    author='dev',
    author_email='contact@oslandia.com',
    license='GPLv3',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 2.7',
    ],
    packages=find_packages(),
    install_requires=requirements
)
