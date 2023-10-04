from setuptools import setup, find_namespace_packages


setup(
    name='auto-irida-uploader',
    version='0.1.0-alpha',
    packages=find_namespace_packages(),
    entry_points={
        "console_scripts": [
            "auto-irida-uploader = auto_irida_uploader.__main__:main",
        ]
    },
    scripts=[],
    package_data={
    },
    install_requires=[
        'iridauploader>=0.9.0',
    ],
    description='Automated upload of sequence data to IRIDA, using the irida-uploader tool.',
    url='https://github.com/BCCDC-PHL/auto-irida-uploader',
    author='Dan Fornika',
    author_email='dan.fornika@bccdc.ca',
    include_package_data=True,
    keywords=[],
    zip_safe=False
)
