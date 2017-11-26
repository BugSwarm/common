from distutils.core import setup

setup(
    name='bugswarmcommon',
    version='0.1',
    description='Internal common libraries for BugSwarm components',
    packages=['bugswarmcommon'],
    install_requires=[
        'requests==2.18.4',
    ],
)
