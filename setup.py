from distutils.core import setup

setup(
    name='quarry',
    version='1.9.3',  # Also update doc/conf.py
    author='Barney Gale',
    author_email='barney@barneygale.co.uk',
    url='https://github.com/barneygale/quarry',
    license='MIT',
    description='Minecraft protocol library',
    long_description=open('README.rst').read(),
    install_requires=[
        'bitstring >= 3.1.0',
        'cached_property >= 1.2.0',
        'twisted >= 22.0.0',
        'cryptography >= 0.9',
        'pyOpenSSL >= 0.15.1',
        'service_identity >= 14.0.0',
        'mutf8 >= 1.0.5',
    ],
    test_requires=[
        'pytest'
    ],
    packages=[
        "quarry",
        "quarry.data",
        "quarry.net",
        "quarry.types",
        "quarry.types.buffer",
    ],
    package_data={'quarry': [
        'data/packets/*.csv',
        'data/data_packs/*.nbt',
        'data/keys/*',
    ]},
)
