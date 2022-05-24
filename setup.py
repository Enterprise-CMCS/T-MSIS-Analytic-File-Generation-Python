from setuptools import find_packages, setup

from taf import __version__

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="taf",
    python_requires=">=3.8",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    long_description=long_description,
    long_description_content_type="text/markdown",
    version=__version__,
    description="A package to generate T-MSIS analytic files using Databricks",
    author="Jesse Beaumont, Scott Cleeton",
    license="CC0 1.0 Universal",
    classifiers=[
        "Programming Language :: Python :: 3.8.6 :: Only",
        "Intended Audience :: Data Engineers",
        "Intended Audience :: Business Analysts",
    ],
    project_urls={
        "Documentation": "https://id.atlassian.com/login?continue=https%3A%2F%2Fcms-dataconnect.atlassian.net%2Flogin%3FredirectCount%3D1%26dest-url%3D%252Fwiki%252Fspaces%252FTDRI%252Foverview%26application%3Dconfluence&application=confluence",
        "Tracker": "https://cms-dataconnect.atlassian.net/jira/software/projects/TDRI/boards/168/backlog",
    },
)
