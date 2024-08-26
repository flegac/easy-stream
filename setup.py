from setuptools import setup

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name='easy-stream',
    version='0.0.1-dev2',
    author='flegac',
    description='Task orchestration library',

    long_description=long_description,
    long_description_content_type="text/markdown",
    url="",
    project_urls={
    },
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    packages=['task_lib', 'stream_lib', 'concurrent_lib'],
    # install_requires=[line for line in open('requirements.txt')],
    python_requires=">=3.7",
    include_package_data=True
)
