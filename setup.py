from __future__ import unicode_literals
from setuptools import setup, find_packages


setup(
    name='sippers',
    version='2.4.0',
    packages=find_packages(),
    url='https://github.com/gisce/sippers',
    license='GPLv3',
    author='GISCE-TI, S.L.',
    author_email='devel@gisce.net',
    description='sippers',
    long_description='sippers',
    entry_points='''
        [console_scripts]
        sippers=sippers.cli:sippers
    ''',
    package_data={
        'sippers': ['data/*']
    },
    install_requires=[
        'raven<6.0.0;python_version<="2.7.18"',
        'raven;python_version>"2.7.18"',
        "pymongo<=3.13.0",
        "osconf",
        "marshmallow<3.0",
        "click"
    ],
    test_suite='tests',
)
