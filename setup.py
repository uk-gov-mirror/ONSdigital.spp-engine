from setuptools import find_packages
from setuptools import setup

setup(
    description = 'Statistical Production Pipeline Engine',
    author = "SPP",
    url = 'https://github.com/ONSdigital/spp-engine',
    version = '0.0.6',
    packages = find_packages(exclude=['tests']),
    scripts = [],
    name = 'spp_engine',
    install_requires = ['es_functions'],
    classifiers = []
)
