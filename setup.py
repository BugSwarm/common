from distutils.core import setup

setup(
    name='bugswarmcommon',
    version='0.1',
    description='Library of common modules used throughout the BugSwarm toolset',
    packages=['bugswarmcommon'],
    install_requires=[
        'requests==2.18.4',
    ],
)
