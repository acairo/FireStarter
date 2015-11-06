import json
import readers
import igniters
import writers
from pyspark import SparkConf, SparkContext
from pprint import pprint as pp

required_config = frozenset(['readers'])
optional_config = frozenset(['igniters', 'writers'])

mappings = {
    "readers": {
      'http_api': readers.HttpApi
    },
    "igniters": {
      'lighter': igniters.Lighter
    },
    "writers": {
      'hdfs': writers.HadoopFileSystem
    }
  }

class FireStarter():

  def __init__(self, config_file):
    self.config_file = config_file

  def read_config_file(self):
    with open(self.config_file, 'r+') as config_data:
      self.config_data = config_data.read()

  def parse_config_contents(self):
    self.config = json.loads(self.config_data)
    check_requirements = required_config - frozenset(self.config.keys())
    if check_requirements:
      raise ValueError('%s must contain %s' % (self.config_file, ', '.join(check_requirements)))

  def load_modules(self):
    """This loop initializes all of the readers,
    writers, and igniters then stores them in an array"""
    self.modules = {}
    self.readers = self.modules['readers'] = []
    self.igniters = self.modules['igniters'] = []
    self.writers = self.modules['writers'] = []

    for module_type, module_list in self.config.items():
      mapping = mappings[module_type]
      for module in module_list:
        # Access the module via name, or by order
        new_module = self.modules[module['name']] = mapping[module['type']](**module['parameters'])
        self.modules[module_type].append(new_module)

  def create_spark_context(self):
    conf = self.config['spark_conf']
    self.spark_config = SparkConf()
    self.spark_config.setAppName(conf['app_name'])
    for attribute, value in conf['parameters'].items():
        self.spark_config.set(attribute, value)

    self.sc = SparkContext(conf = self.spark_config)

  def run(self):
    self.read_config_file()
    self.parse_config_contents()
    self.load_modules()
