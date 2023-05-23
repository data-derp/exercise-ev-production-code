from setuptools import setup, find_packages
from os.path import basename
from os.path import splitext
from glob import glob

basedir = "delta-lake"

with open(f'{basedir}/requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='exercise_ev_production_code_delta_lake',
    version='0.1.0',
    description='Exercise EV Production Code Delta Lake',
    author='Kelsey Beyer',
    author_email='kelseymok@gmail.com',
    url='https://github.com/data-derp/exercise-ev-productino-code',
    packages=find_packages(f'{basedir}/src', exclude=('tests')),
    package_dir={'': f'{basedir}/src'},
    py_modules=[splitext(basename(path))[0] for path in glob(f'{basedir}/src/*.py')],
    install_requires=required
)
