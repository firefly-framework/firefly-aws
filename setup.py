import setuptools
from setuptools.command.develop import develop
from setuptools.command.install import install

with open("README.md", "r") as fh:
    long_description = fh.read()


setuptools.setup(
    name='firefly_aws',
    version='0.1',
    author="",
    author_email="",
    description="Put project description here.",
    long_description=long_description,
    url="",
    entry_points={
        'console_scripts': ['firefly=firefly.presentation.cli:main']
    },
    install_requires=[
        'boto3>=1.12.42',
        'cognitojwt>=1.2.2',
        'firefly-dependency-injection>=0.1',
        'requests>=2.23.0',
        'troposphere>=2.6.1',
    ],
    packages=setuptools.PEP420PackageFinder.find('src'),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent",
    ]
)
