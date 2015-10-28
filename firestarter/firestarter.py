import json
import readers
import igniters
import writers

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
    self.modules = {}
    self.readers = self.modules['readers'] = []
    self.igniters = self.modules['igniters'] = []
    self.writers = self.modules['writers'] = []

    for module_type, module_list in self.config.items():
      mapping = mappings[module_type]
      object_store = self.modules[module_type]
      for module in module_list:
        new_module = mapping[module['type']](module['parameters'])
        object_store.append(new_module)

  def run(self):
    self.read_config_file()
    self.parse_config_contents()
    self.load_modules()
