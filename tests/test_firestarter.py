from unittest import TestCase
from unittest import main as run_tests
from mock import mock_open, patch, MagicMock
from ..firestarter.firestarter import FireStarter
from ..firestarter.readers import HttpApi
from ..firestarter.igniters import Lighter
from ..firestarter.writers import HadoopFileSystem
import json


class TestFireStarter(TestCase):

  def setUp(self):
    self.config_file = 'config_file.json'
    with patch.object(FireStarter, 'run', return_value=True):
      self.firestarter = FireStarter(self.config_file)
      self.firestarter.run = MagicMock()
      self.firestarter.config_data = 'foo'

  def test_init(self):
    with patch.object(__builtins__, 'open', mock_open(read_data='foo')):
      self.assertEqual(self.firestarter.config_file, self.config_file)

  def test_read_config_file(self):
    config_data = '{"json":"data"}'

    with patch.object(__builtins__, 'open', mock_open(read_data=config_data)):
      self.firestarter.read_config_file()
      self.assertEqual(self.firestarter.config_data, config_data)

  def test_parse_config_contents(self):
    config = {'readers': 'reader_type'}

    with patch.object(json, 'loads', return_value=config):
      self.firestarter.parse_config_contents()
      self.assertEqual(self.firestarter.config, config)

  def test_parse_config_contents_throws_value_error(self):
    config = {'no_readers_defined': 'reader_type'}

    with patch.object(json, 'loads', return_value=config):
      with self.assertRaises(ValueError):
        self.firestarter.parse_config_contents()

  # def test_load_modules(self):
  #   self.firestarter.config = {'readers': [{'type': 'http_api', 'parameters': {}}]}

  #   with patch.object(HttpApi, '__init__', return_value=None):
  #     self.firestarter.load_modules()
  #     self.assertIsInstance(self.firestarter.readers[0], HttpApi)

  def test_load_all_modules(self):
    self.firestarter.config = {'readers': [{'type': 'http_api', 'parameters': {}}],
      'igniters': [{'type': 'lighter', 'parameters': {}}],
      'writers': [{'type': 'hdfs', 'parameters': {}}]}

    with patch.object(HttpApi, '__init__', return_value=None):
      with patch.object(Lighter, '__init__', return_value=None):
        with patch.object(HadoopFileSystem, '__init__', return_value=None):
          self.firestarter.load_modules()
          self.assertIsInstance(self.firestarter.readers[0], HttpApi)
          self.assertIsInstance(self.firestarter.igniters[0], Lighter)
          self.assertIsInstance(self.firestarter.writers[0], HadoopFileSystem)

if __name__ == '__main__':
  run_tests()