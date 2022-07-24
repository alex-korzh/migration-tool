from setuptools import setup, find_packages

setup(
    name='migration tool',
    version='0.0.4',
    author='Alex',
    author_email='alex@korzh.me',
    url='',
    description='Migration tool.',
    packages=find_packages(),
    entry_points={
        'console_scripts': [
            'migrate = migrate.migrate:run'
        ]
    },
    install_requires=['asyncpg'],
    zip_safe=False
)
