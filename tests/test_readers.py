from unittest import TestCase
from unittest import main as run_tests
from mock import mock_open, patch, MagicMock
from ..firestarter.firestarter import FireStarter
from ..firestarter.readers import HttpApi
from ..firestarter.readers import Reader
from ..firestarter.igniters import Lighter
from ..firestarter.writers import HadoopFileSystem
import json


class TestReader(TestCase):

  def setUp(self):
    #self.config_file = 'config_file.json'
    #with patch.object(FireStarter, 'run', return_value=True):
    #  self.firestarter = FireStarter(self.config_file)
    #  self.firestarter.run = MagicMock()
    #  self.firestarter.config_data = 'foo'
    self.reader = Reader()

  def test_reader(self):
    self.assertIsNotNone(self.reader)

  def test_read(self):
    with self.assertRaises(NotImplementedError):
      self.reader.read()
      self.assertNone(self.reader.data)
  
  def test_head_data(self):
    self.reader.data = [0,1,2,3,4,5,6,7,8,9,10] 
    head_result = self.reader.head_data()
    self.assertEquals(head_result, self.reader.data[0:9])
  
if __name__ == '__main__':
    run_tests(verbosity=3)
