""" Setup file for statscache """

from setuptools import setup


def get_description():
    with open('README.rst', 'r') as f:
        return ''.join(f.readlines()[2:])

requires = [
    'statscache',
]

tests_require = [ ]

setup(
    name='statscache_plugins',
    version='0.0.1',
    description='Plugins for statscache, the fedmsg statistics daemon',
    long_description=get_description(),
    author='Ralph Bean',
    author_email='rbean@redhat.com',
    url="https://github.com/fedora-infra/statscache_plugins/",
    download_url="https://pypi.python.org/pypi/statscache_plugins/",
    license='LGPLv2+',
    install_requires=requires,
    tests_require=tests_require,
    test_suite='nose.collector',
    packages=['statscache_plugins'],
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        'Environment :: Web Environment',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Intended Audience :: Developers',
        'Programming Language :: Python',
    ],
    entry_points={
        'statscache.plugin': [
            "volume = statscache_plugins.volume.simple",
            "releng = statscache_plugins.releng",
            #"volume_by_topic = statscache_plugins.volume.by_topic",
            "volume_by_category = statscache_plugins.volume.by_category",
            #"volume_by_user = statscache_plugins.volume.by_user",
            #"volume_by_package = statscache_plugins.volume.by_package",
        ]
    },
)
