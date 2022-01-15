from setuptools import setup, find_packages


with open("README.md", "r") as readme_file:
    readme = readme_file.read()

requirements = ["pandas", "valdec", "pydantic"]

setup(
    name="data_quality",
    version="0.0.01",
    author="Customer and Business Analytics",
    author_email="stefano.gelli@enel.com",
    description="",
    long_description=readme,
    long_description_content_type="text/markdown",
    url="https://github.com/enelx-customer-business-analytics/enelx_utils.git",
    packages=find_packages(exclude=("data_quality.src")),
    install_requires=requirements,
    include_package_data=True,
    package_data={
        },
    classifiers=[
        "Programming Language :: Python :: 3.8",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
    ],
)