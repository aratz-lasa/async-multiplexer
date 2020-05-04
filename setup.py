import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

install_requires = [
    "uvarint==1.2.0",
]

extras_require = {
    "dev": [
        "pytest==5.4.1",
        "pytest-asyncio==0.11.0",
        "hypothesis==5.10.4",
        "hypothesis-pytest==0.19.0",
    ],
}

setuptools.setup(
    name="async_multiplexer",
    version="0.1.0",
    author="Aratz M. Lasa",
    author_email="aratz.m.lasa@gmail.com",
    description="Asyncio TCP Multiplexer",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/aratz-lasa/async-multiplexer",
    install_requires=install_requires,
    extras_require=extras_require,
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires=">=3.8",
)
