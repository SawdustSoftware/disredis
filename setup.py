from distutils.core import setup
from setuptools import find_packages

setup(
    name='disredis',
    version='1.0',
    author='CustomMade Ventures',
    author_email='sawdust@custommade.com',
    packages=find_packages(),
    license='LICENSE.txt',
    description='Distributed Redis Client (dis-redis) to enable real-time failover of redis masters to paired slaves',
    long_description=open('README.txt').read(),
    install_requires=[
        'Django >= 1.4.1',
        'redis >= 2.7.6'
    ],
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.6',
        'Programming Language :: Python :: 2.7',
        'Operating System :: OS Independent',
        'Topic :: Software Development :: Libraries',
        'Topic :: Utilities',
        'Environment :: Web Environment',
        'Framework :: Django',
    ],   url='https://github.com/SawdustSoftware/disredis',
    zip_safe=False
)
