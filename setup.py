from setuptools import setup, find_packages


setup(
    name='sippers',
    version='0.1.0',
    packages=find_packages(),
    url='https://github.com/gisce/sippers',
    license='GPLv3',
    author='GISCE-TI, S.L.',
    author_email='devel@gisce.net',
    description='',
    entry_points="""
        [console_scripts]
        sippers=sippers.sippers:main
    """,
)
