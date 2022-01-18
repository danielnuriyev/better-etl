import json
import setuptools

with open("requirements.json") as fp:
    requirements = json.load(fp)

setuptools.setup(
    name="datadagster",
    version="0.0.1",
    description="",
    long_description="",
    long_description_content_type="text/markdown",
    author="Daniel Nuriyev",
    packages=setuptools.find_packages(where="src"),
    package_dir={"": "src"},
    scripts=[],
    install_requires=requirements["install_requires"],
    include_package_data=True,
    extras_require=requirements["extras_require"],
    python_requires=">=3.10.1",
)
