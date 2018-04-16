from distutils.core import setup

with open('README.md') as f:
    readme = f.read()

setup(
    name='bugswarm-common',
    version='0.0.1',
    description='Library of modules used throughout the BugSwarm toolset',
    long_description=readme,
    license='BSD',
    author='BugSwarm',
    author_email='dev.bugswarm@gmail.com',
    url='https://github.com/BugSwarm/common',
    classifiers=[
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: BSD License',
    ],
    zip_safe=False,
    packages=[
        'bugswarm.common',
    ],
    install_requires=[
        'requests==2.18.4',
        'CacheControl==0.12.3',
        'requests-cache==0.4.13',
    ],
)
