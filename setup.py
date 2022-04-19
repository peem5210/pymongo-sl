from setuptools import setup, find_packages

VERSION = '0.0.5'
DESCRIPTION = 'Like pymongo, but have cache logic on top to store a location or whatever'
LONG_DESCRIPTION = 'Like pymongo, but have cache logic on top to store a location or whatever'

setup(
    name="snaplogic_pymg",
    version=VERSION,
    author="PassawitPunyawat",
    author_email="passawit@punyawat.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    package_dir={"": "src"},
    packages=find_packages(where='src'),
    project_url="https://github.com/peem5210/snaplogic-pymg",
    keywords=['HelloHello'],
    classifiers=[
        "Intended Audience :: Education",
        "Programming Language :: Python :: 2",
    ]
)
