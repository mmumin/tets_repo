import setuptools
setuptools.setup(
        name='aws_eb_util',
        version='0.0.8',
        description='AWS Event Bridge Package',
        url='https://github.com/mmumin/tets_repo',
        author='dummy',
        install_requires=['boto3==1.16.10', 'python-decouple==3.4'],
        author_email='mumin.mohammed@gmail.com',
        packages=setuptools.find_packages(),
        zip_safe=False
        )

