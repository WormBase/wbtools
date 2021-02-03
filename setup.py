import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="wbtools",
    version="1.0.6",
    author="Valerio Arnaboldi",
    author_email="valearna@caltech.edu",
    description="Interface to WormBase (www.wormbase.org) curation data, including literature management and NLP "
                "functions",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/WormBase/wbtools",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
    install_requires=[
        'psycopg2-binary',
        'numpy~=1.19.2',
        'fabric~=2.5.0',
        'gensim~=3.8.3',
        'nltk~=3.5',
        'setuptools~=50.3.2',
        'PyMuPDF~=1.18.4'
    ]
)
