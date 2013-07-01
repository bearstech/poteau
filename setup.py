from setuptools import setup

setup(name='poteau',
      version='0.1',
      author='Mathieu Lecarme',
      author_email='mlecarme@bearstech.com',
      license='BSD',
      packages=['poteau'],
      scripts=['scripts/poteau-web'],
      install_requires=['pyyaml',
                'ua-parser',
                'lamson',
                'pyelasticsearch',
                'docopt >=0.6'
                ]
      )
