from setuptools import setup, find_packages


REQUIRES_INSTALL = [
    "numba==0.56.0",
    "numpy==1.22.4",
    "openpyxl==3.0.10",
    "lxml==4.9.1",
    "pyarrow==9.0.0",
    "tabulate==0.8.10",
    "pandas==1.4.3",
    "python-dateutil==2.8.2",
    "retry==0.9.2",
]


setup(
    name="mercadata",
    version="0.0.1",
    author_email="dados@mercafacil.com.br",
    url="https://github.com/mercafacil/mercadata",
    packages=find_packages(),
    include_package_data=True,
    install_requires=REQUIRES_INSTALL,
)
