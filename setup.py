from setuptools import setup

setup(name='thrift_amqp_tornado',
      version='0.2.6',
      description='Thirft transport implementation over the AMQP protocol',
      author='Alexis Montagne',
      author_email='alexis.montagne@upfluence.co',
      url='https://github.com/upfluence/thrift-amqp-tornado',
      packages=['thrift_amqp_tornado'],
      install_requires=['thrift', 'pika-upfluence', 'toro', 'futures'])
