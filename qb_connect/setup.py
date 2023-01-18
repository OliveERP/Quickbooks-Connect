from setuptools import setup, find_packages

with open("requirements.txt") as f:
	install_requires = f.read().strip().split("\n")

# get version from __version__ variable in qb_connect/__init__.py
from qb_connect import __version__ as version

setup(
	name="qb_connect",
	version=version,
	description="Quickbooks and ERPNext two-way sync connector",
	author="WebMekanics",
	author_email="talal.hassan@webmekanics.com",
	packages=find_packages(),
	zip_safe=False,
	include_package_data=True,
	install_requires=install_requires
)
