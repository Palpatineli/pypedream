from setuptools import setup, find_packages

import subprocess

try:  # Try to create an rst long_description from README.md
    args = "pandoc", "--to", "rst", "README.md"
    long_description = subprocess.check_output(args)
    long_description = long_description.decode()
except Exception as error:
    print("README.md conversion to reStructuredText failed. Error:\n",
          error, "Setting long_description to None.")
    long_description = None

setup(
    name='pypedream',
    version='0.1.0',
    packages=find_packages(),
    author='Keji Li',
    author_email='mail@keji.li',
    install_requires=["multiprocessing_logging"],
    include_package_data=True,
    tests_require=["pytest", "pytest-runner"],
    extras_require={"print": ["print-tree2"], "draw": ["pygraphviz", "networkx"]},
    description='matplotlib customizations and customized ploting functions',
    long_description=long_description
)
