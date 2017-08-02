from setuptools import setup

setup(
    name='celery-singleton',
    version='0.1.3',
    description='Prevent duplicate celery tasks',
    author='Steinthor Palsson',
    author_email='steini90@gmail.com',
    url='https://github.com/steinitzu/celery-singleton',
    license='MIT',
    install_requires=[
        'celery>=4.0.0',
        'redis>=2.10.5'
    ],
    packages=['celery_singleton']
)
