from setuptools import setup, find_packages
version = '0.1'

install_requirements = \
    ['cryptography',
     'psycopg2-binary',
     'sqlalchemy',
     'mysqlclient',
     'pymongo',
     'pandas',
     'celery',
     'boto3']

test_requirements = \
    ['pytest', 'pytest-cov']

setup(
    name='checkdt',
    version=version,
    description='Simplify ETL workflow',
    packages=find_packages(),
    include_package_data=True,
    install_requires=install_requirements,
    setup_requires=[],
    tests_require=test_requirements,
    python_requires='>=3.6',
    author='Tejas',
    zip_safe=True
)
