from distutils.core import setup

with open('README.md') as f:
    readme = f.read()

setup(
    name='databricks_kernel',
    version='0.2',
    packages=['databricks_kernel'],
    description='Databricks kernel for jupyter',
    long_description=readme,
    author='Mark Baas',
    author_email='mbaas@hso.com',
    url='https://github.com/hso-nn/databricks_kernel',
    install_requires=[
        'jupyter_client', 'IPython', 'ipykernel'
    ],
    classifiers=[
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
    ],
    include_package_data=True
)
