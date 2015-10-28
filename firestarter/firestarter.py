import json
import readers
import igniters
import writers

required_config = frozenset(['readers'])
optional_config = frozenset(['igniters', 'writers'])

reader_mapping = {
  'http_api': readers.HttpApi
}

igniter_mapping = {
  'lighter': igniters.Lighter
}

writer_mapping = {
  'hdfs': writers.HadoopFileSystem
}

module_mappings = [reader_mapping, igniter_mapping, writer_mapping]

class FireStarter():

  def __init__(self, config_file):
    self.config_file = config_file
    self.readers = []
    self.igniters = []
    self.writers = []
    self._config_data_map = {"readers": {"mapping": reader_mapping, "module_list": self.readers},
      "igniters": {"mapping": igniter_mapping, "module_list": self.igniters},
      "writers": {"mapping": writer_mapping, "module_list": self.writers}}
    self.run()

  def read_config_file(self):
    with open(self.config_file, 'r+') as config_data:
      self.config_data = config_data.read()

  def parse_config_contents(self):
    self.config = json.loads(self.config_data)
    check_requirements = required_config - frozenset(self.config.keys())
    if check_requirements:
      raise ValueError('%s must contain %s' % (self.config_file, ', '.join(check_requirements)))

  def load_modules(self):
    for config_key, config in self.config.items():
      mapping = self._config_data_map[config_key]['mapping']
      module_array = self._config_data_map[config_key]['module_list']
      module = mapping.get(config['type'])
      new_class = module(config['parameters'])
      module_array.append(new_class)

  def run(self):
    self.read_config_file()
    self.parse_config_contents()
    self.load_modules()
