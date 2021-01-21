import setuptools


setuptools.setup(
    name='firefly-aws',
    version='1.1.20',
    author="",
    author_email="",
    description="AWS extension for the Firefly framework.",
    url="",
    entry_points={
        'console_scripts': ['firefly=firefly.presentation.cli:main']
    },
    install_requires=[
        'boto3>=1.12.42',
        'cognitojwt>=1.2.2',
        'dateparser>=0.7.4',
        'firefly-dependency-injection>=1.0.0',
        'firefly-framework>=1.1.33',
        'Jinja2>=2.11.1',
        'jinjasql>=0.1.8',
        'requests>=2.23.0',
        'troposphere>=2.6.1',
    ],
    data_files=[('firefly_aws_config', ['firefly.yml'])],
    packages=setuptools.PEP420PackageFinder.find('src'),
    package_dir={'': 'src'},
    classifiers=[
        "Programming Language :: Python :: 3.7",
        "Operating System :: OS Independent",
    ]
)
