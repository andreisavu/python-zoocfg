
try:
    from setuptools import setup
except ImportError:
    from distutils import setup

setup(
    name='zoocfg',
    version='0.1.0',
    description='ZooKeeper config parser and validator',
    author='Andrei Savu',
    author_email='contact@andreisavu.ro',
    license='Apache License 2.0',
    url='http://github.com/andreisavu/python-zoocfg',
    scripts=['zoocfg.py'],
    test_suite='test'
)

