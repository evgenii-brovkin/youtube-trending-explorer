import setuptools


setuptools.setup(
    name="trendscrap",
    version="1.0.0",
    author="Evgenii Brovkin",
    description="A little scrapper of Youtube Trening page",
    url="https://github.com/evgenii-brovkin/youtube-trending-explorer",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.6',
)