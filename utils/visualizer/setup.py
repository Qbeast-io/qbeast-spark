import setuptools


setuptools.setup(
    name="qviz",
    version="0.0.1",
    author="Qbeast",
    description="CI tool for OTree index visualization",
    packages=setuptools.find_packages(),
    entry_points={
        "console_scripts": [
            "qviz = qviz:show_tree"
        ]
    }
)
