from setuptools import setup, find_packages


setup(
    name='sippers',
    version='1.1.1',
    packages=find_packages(),
    url='https://github.com/gisce/sippers',
    license='GPLv3',
    author='GISCE-TI, S.L.',
    author_email='devel@gisce.net',
    description='',
    entry_points='''
        [console_scripts]
        sippers=sippers.cli:sippers
    ''',
    package_data={
        'sippers': ['data/*']
    },
    install_requires=[
        "raven",
        "pymongo<3.0",
        "osconf",
        "marshmallow<3.0",
        "click"
    ],
    test_suite='tests',
)
