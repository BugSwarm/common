from setuptools import setup
from setuptools import find_packages

setup(
    name='bugswarm-common',
    version='0.1.15',
    url='https://github.com/BugSwarm/common',
    author='BugSwarm',
    author_email='dev.bugswarm@gmail.com',

    description='Library of modules used throughout the BugSwarm toolset',
    long_description='Library of modules used throughout the BugSwarm toolset',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: BSD License',
    ],
    zip_safe=False,
    packages=find_packages(),
    namespace_packages=[
        'bugswarm',
    ],
    install_requires=[
        'requests>=2.20.0',
        'CacheControl==0.12.3',
        'requests-cache==0.4.13',
    ],
)
