from setuptools import setup
from setuptools import find_packages

with open('README.md') as f:
    readme = f.read()

setup(
    name='bugswarm-common',
    version='0.0.2',
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
        'requests==2.18.4',
        'CacheControl==0.12.3',
        'requests-cache==0.4.13',
    ],
)
