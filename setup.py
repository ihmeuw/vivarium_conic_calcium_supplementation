#!/usr/bin/env python
import os

from setuptools import setup, find_packages


if __name__ == "__main__":

    base_dir = os.path.dirname(__file__)
    src_dir = os.path.join(base_dir, "src")

    about = {}
    with open(os.path.join(src_dir, "vivarium_conic_calcium_supplementation", "__about__.py")) as f:
        exec(f.read(), about)

    with open(os.path.join(base_dir, "README.rst")) as f:
        long_description = f.read()

    install_requirements = [
        'vivarium==0.9.3',
        'vivarium_public_health==0.10.3',
        'vivarium_cluster_tools==1.1.2',
        'vivarium_inputs[data]==3.1.0',
        
        # These are pinned for internal dependencies on IHME libraries
        'numpy<=1.15.4',
        'tables<=3.4.0',
        'pandas<0.25',        
        
        'click',
        'scipy',
        'matplotlib',
        'seaborn',
        'jupyter',
        'jupyterlab',
        'pytest',
        'pytest-mock',
        'pyyaml',
    ]

    setup(
        name=about['__title__'],
        version=about['__version__'],

        description=about['__summary__'],
        long_description=long_description,
        license=about['__license__'],
        url=about["__uri__"],

        author=about["__author__"],
        author_email=about["__email__"],

        package_dir={'': 'src'},
        packages=find_packages(where='src'),
        include_package_data=True,

        install_requires=install_requirements,

        zip_safe=False,

        entry_points='''
            [console_scripts]
            make_specs=vivarium_conic_calcium_supplementation.tools.cli:make_specs
        '''
    )
