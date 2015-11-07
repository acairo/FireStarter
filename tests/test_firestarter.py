from unittest import TestCase
from unittest import main as run_tests
from mock import mock_open, patch, MagicMock
from ..firestarter.firestarter import FireStarter
from ..firestarter.readers import HttpApi
from ..firestarter.igniters import Lighter
from ..firestarter.writers import HadoopFileSystem
from ..firestarter.pyspark import SparkConf, SparkContext
import json

class TestFireStarter(TestCase):

  def setUp(self):
    self.config_file = 'config_file.json'
    self.firestarter = FireStarter(self.config_file)
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
    config = {'modules': {}}

    with patch.object(json, 'loads', return_value=config):
      self.firestarter.parse_config_contents()
      self.assertEqual(self.firestarter.config, config)

  def test_parse_config_contents_throws_value_error(self):
    config = {'no_readers_defined': {}}

    with patch.object(json, 'loads', return_value=config):
      with self.assertRaises(ValueError):
        self.firestarter.parse_config_contents()

  def test_load_all_modules(self):
    self.firestarter.config = {"modules": [{"name": "my_rest_api","type": "http_api","parameters": {"url": "http://drunken.guru/"}},{"name": "hive_query","type": "http_api","parameters": {"url": "http://original.guru/"}},{"name": "crunch_the_numbers","type": "lighter","parameters": {"math_rules": "2+2=4"}},{"name": "data_center_cluster","type": "hdfs","parameters": {"hive_table": "mydb.table.name"}}]}
    with patch.object(HttpApi, '__init__', return_value=None):
      with patch.object(Lighter, '__init__', return_value=None):
        with patch.object(HadoopFileSystem, '__init__', return_value=None):
          mock_http_api = MagicMock()
          mock_lighter = MagicMock()
          mock_hdfs = MagicMock()
          
          mock_http_api.data = ['hello']
          mock_lighter.data = ['world']
          mock_hdfs.data = ['yall']

          self.firestarter.mappings = mappings = {'http_api': mock_http_api, 'lighter': mock_lighter, 'hdfs': mock_hdfs}
          self.firestarter.load_modules()
          self.assertIn('my_rest_api', self.firestarter.modules)
          self.assertEqual(id(self.firestarter.modules['my_rest_api'].data), id(mock_http_api.data))
          self.assertIn('hive_query', self.firestarter.modules)
          self.assertIn('data_center_cluster', self.firestarter.modules)
          self.assertIn('my_rest_api', self.firestarter.data)
          self.assertIn('hive_query', self.firestarter.data)
          self.assertIn('data_center_cluster', self.firestarter.data)

  def test_create_spark_context(self):
    self.firestarter.config = {"spark_conf": {"app_name": "Fill Your Mother", "parameters": {"num_executors:": 4}}}
    with patch.object(SparkConf, '__init__', return_value=None) as mock_spark_conf:
      with patch.object(SparkConf, 'set', return_value=True) as mock_set:
        with patch.object(SparkConf, 'setAppName', return_value=True) as mock_set_app_name:
          with patch.object(SparkContext, '__init__', return_value=None) as mock_spark_context:
            self.firestarter.create_spark_context()
            mock_set_app_name.assert_called_once_with("Fill Your Mother")
            mock_set.assert_called_once_with(*set(["num_executors:", 4]))
            mock_spark_context.assert_called_once_with(**{"conf": self.firestarter.spark_config})
            self.assertIsInstance(self.firestarter.sc, SparkContext)

  def test_execute(self):
    self.firestarter.read_config_file = MagicMock()
    self.firestarter.parse_config_contents = MagicMock()
    self.firestarter.load_modules = MagicMock()
    self.firestarter.run_modules = MagicMock()

    self.firestarter.execute()
    self.firestarter.read_config_file.assert_called_once_with()
    self.firestarter.parse_config_contents.assert_called_once_with()
    self.firestarter.load_modules.assert_called_once_with()
    self.firestarter.run_modules.assert_called_once_with()

  # #integration test!
  # def test_load_all_modules(self):
  #   self.firestarter.config = {"readers": [{"name": "my_rest_api","type": "http_api","parameters": {"url": "http://drunken.guru/"}}, {"name": "hive_query","type": "http_api","parameters": {"url": "http://original.guru/"}}],"igniters": [{"name": "crunch_the_numbers","type": "lighter","parameters": {"math_rules": "2+2=4"}}],"writers": [{"name": "data_center_cluster","type": "hdfs","parameters": {"hive_table": "mydb.table.name"}}]}
  #   with patch.object(Lighter, '__init__', return_value=None):
  #     with patch.object(HadoopFileSystem, '__init__', return_value=None):
  #       self.firestarter.load_modules()
  #       self.assertIsInstance(self.firestarter.readers[0], HttpApi)
  #       self.assertIsInstance(self.firestarter.readers[1], HttpApi)
  #       self.assertIsInstance(self.firestarter.igniters[0], Lighter)
  #       self.assertIsInstance(self.firestarter.writers[0], HadoopFileSystem)
  #       self.assertIsInstance(self.firestarter.modules['my_rest_api'], HttpApi)
  #       self.assertIsInstance(self.firestarter.modules['hive_query'], HttpApi)
  #       self.assertIsInstance(self.firestarter.modules['readers'][0], HttpApi)
  #       self.assertIsInstance(self.firestarter.modules['igniters'][0], Lighter)
  #       self.assertIsInstance(self.firestarter.modules['writers'][0], HadoopFileSystem)
  #       self.assertEqual(self.firestarter.modules['my_rest_api'], self.firestarter.readers[0])

if __name__ == '__main__':
  run_tests()
