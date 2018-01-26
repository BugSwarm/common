from distutils.core import setup

setup(
    name='bugswarm-common',
    version='0.1',
    description='Library of common modules used throughout the BugSwarm toolset',
    packages=['bugswarmcommon'],
    install_requires=[
        'requests==2.18.4',
        'CacheControl==0.12.3',
        'requests-cache==0.4.13',
    ],
)
