from distutils.core import setup
setup(
  name = 'hexagon',
  packages = ['hexagon'], # this must be the same as the name above
  version = '0.1',
  description = 'a graph databased backed by LevelDB',
  author = 'Daniel Pyrathon',
  author_email = 'pirosb3@gmail.com',
  url = 'https://github.com/PirosB3/Hexagon', # use the URL to the github repo
  download_url = 'https://github.com/PirosB3/Hexagon/archive/master.zip', # I'll explain this in a second
  install_requires=['leveldb'],
  keywords = ['leveldb', 'graph'], # arbitrary keywords
)
