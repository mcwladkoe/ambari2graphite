from setuptools import setup, find_packages

requires = [
    'xlsxwriter',
    'requests',
    'graphyte',
    # 'csv',
]

setup(
    name='ambari2graphite',
    version='0.1.0',
    description='',
    author='McWladkoE',
    author_email='svevladislav@gmail.com',
    url='',
    packages=find_packages('src'),
    package_dir={'': 'src'},
    install_requires=requires,
    entry_points="""\
        [console_scripts]
        ambari2graphite = ambari2graphite.__main__:main
    """,
)
