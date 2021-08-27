from setuptools import setup, find_packages

setup(
    name="tagr",
    packages=find_packages(exclude=("test", "test.*")),
    version="0.0.1",
    license="MIT",
    long_description="Cloud Agnostic Data Science Productivity Tool",
    author="Sunrise Long",
    author_email="sunrise.long@yahoo.ca",
    url="https://github.com/tagr-dev/tagr",
    install_requires=["numpy", "pandas", "boto3"],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
