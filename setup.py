from setuptools import setup

setup(name='asyncpg-connect',
      version='0.1.1',
      description='Utilities to make the use of asyncpg easier in your project or script',
      url='http://github.com/merfrei/asyncp-connect',
      author='Emiliano M. Rudenick',
      author_email='emr.frei@gmail.com',
      license='MIT',
      packages=['asyncpg_connect'],
      install_requires=[
          'asyncpg',
      ],
      zip_safe=False)
