from setuptools import find_packages, setup

setup(
    name="whatstheodds-package",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        "omegaconf>=2.3.0",
    ],
    package_data={
        "configs": ["*.yaml", "*.yml"],
    },
    include_package_data=True,
    author="James",
    description="Handles the searching and downloading of match odds.",
)
