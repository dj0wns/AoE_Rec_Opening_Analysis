"""Setup."""
from setuptools import setup, find_packages

setup(
    name='aoe_opening_data',
    version='1.0.6',
    description='Extract opening information for AoE2:DE replays',
    url='https://github.com/dj0wns/AoE_Rec_Opening_Analysis',
    license='MIT',
    author='dj0wns',
    author_email='derekjones@asu.edu',
    packages=find_packages(),
    install_requires=[
      'construct==2.8.16',
      'mgz>=1.7.5',
      'python-dotenv==0.20.0',
      'requests==2.27.1',
    ],
    package_data={
      '': ['aoe2techtree/data/*.json', 'aoe2techtree/data/locale/en/*.json]
    },
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 3',
    ]
)
