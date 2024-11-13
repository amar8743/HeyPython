from setuptools import setup, find_packages

setup(
    name='HeyPython',
    version='0.2',
    packages=find_packages(),
    install_requires=[
        'pandas',
        'oracledb'
    ],
    description="A Python package for performing data operations and interacting with Oracle DB",
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    author="Vinit",
    author_email="vinittest@example.com",
    url="https://github.com/vkgramon1/HeyPython",
)
