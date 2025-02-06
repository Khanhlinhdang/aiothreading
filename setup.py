import re

from pathlib import Path

from setuptools import find_packages, setup

this_directory = Path(__file__).parent

__author__ = re.search(
    r'__author__\s*=\s*"(.*?)"',
    (this_directory / "aiothreading" / "__init__.py").read_text(),
)[1]
__version__ = re.search(
    r'__version__\s*=\s*"(.*?)"',
    (this_directory / "aiothreading" / "__version__.py").read_text(),
)[1]

long_description = (this_directory / "readme.md").read_text()

if __name__ == "__main__":
    setup(
        name="aiothreading",
        author=__author__,
        version=__version__,
        packages=find_packages(),
        include_package_data=True,
        install_requires=["aiologic>=0.13.0"],
        description="AsyncIO version of the standard threading module",
        long_description=long_description,
        long_description_content_type="text/markdown",
        keywords=["aiothreading", "threading", "asyncio"],
        classifiers=[
            "Development Status :: 4 - Beta",
            "Framework :: AsyncIO",
            "Intended Audience :: Developers",
            "Topic :: Software Development :: Libraries",
            "Programming Language :: Python :: 3.8",
            "Programming Language :: Python :: 3.9",
            "Programming Language :: Python :: 3.10",
            "Programming Language :: Python :: 3.11",
            "Programming Language :: Python :: 3.12",
        ]
    )
