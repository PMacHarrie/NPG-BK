from os.path import join, dirname
from setuptools import setup, find_packages


VERSION = (0, 6)
__version__ = VERSION
__versionstr__ = '.'.join(map(str, VERSION))

install_requires = [
    'requests>=2.18.0',
]

setup(
    name = "nderest",
    version = __versionstr__,
    description = "Python client for NDE REST APIs",
    author = "Hieu Phung",
    author_email="hieu.phung@noaa.gov",
    url = "tbd",
    long_description = "Python client for NDE REST APIs, see README for more details",
    packages = find_packages(),
    install_requires = install_requires
)