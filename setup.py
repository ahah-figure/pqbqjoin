from setuptools import setup, find_packages

setup(
    name="pqbqjoin",
    version="0.1",
    description="",
    author="Andrew Hah",
    author_email="hahdawg@yahoo.com",
    license="MIT",
    packages=find_packages(),
    include_package_data=True,
    package_data={
        "": ["*.sh", "*.yaml"]
    },
    install_requires=[
        "dask[complete]>=2021.8.1",
        "pyarrow",
        "gcsfs",
    ],
    tests_require=[
        "numpy",
        "pandas",
        "pytest"
    ],
    zip_safe=False
)
