from distutils.core import setup

setup(
    name="tagr",
    packages=["tagr"],
    version="0.0.1",
    license="MIT",
    description="Cloud Agnostic Data Science Productivity Tool",
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
