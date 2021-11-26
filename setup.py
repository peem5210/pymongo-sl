from setuptools import setup, find_packages

VERSION = '0.0.1'
DESCRIPTION = 'Like pymongo, but have cache logic on top to store a location or whatever'
LONG_DESCRIPTION = 'Like pymongo, but have cache logic on top to store a location or whatever'

setup(
    name="pymongo_sl",
    version=VERSION,
    author="PassawitPunyawat",
    author_email="passawit@punyawat.com",
    description=DESCRIPTION,
    long_description=LONG_DESCRIPTION,
    package_dir={"": "src"},
    packages=find_packages(where='src'),
    project_url="https://github.com/peem5210/pymongo-sl",
    keywords=['HelloHello'],
    classifiers=[
        "Development Status :: Develop",
        "Intended Audience :: Education",
        "Programming Language :: Python :: 3",
        "Operating System :: MacOS :: MacOS X",
    ]
)
