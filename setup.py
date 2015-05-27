from setuptools import setup, find_packages


setup(
    name='sippers',
    version='0.2.0',
    packages=find_packages(),
    url='https://github.com/gisce/sippers',
    license='GPLv3',
    author='GISCE-TI, S.L.',
    author_email='devel@gisce.net',
    description='',
    install_requires=[
        "raven",
        "pymongo<3.0",
        "osconf",
        "marshmallow>=2.0.0b2"
    ],
    test_suite='tests',
)
