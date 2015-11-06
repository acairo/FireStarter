from unittest import TestCase
from unittest import main as run_tests
from mock import mock_open, patch, MagicMock
from ..firestarter.firestarter import FireStarter
from ..firestarter.readers import Reader, HttpApi
from ..firestarter.igniters import Lighter
from ..firestarter.writers import HadoopFileSystem
import requests
import requests.auth
import json


class TestHttpApi(TestCase):

  def setUp(self):
    pass

  def test_http_api_init(self):
    parameters = {'url': 'http://drunken.guru/'}
    self.reader = HttpApi(**parameters)

    self.assertIsNotNone(self.reader)
    self.assertIsInstance(self.reader, HttpApi)
    self.assertEquals(self.reader.data, [])
    self.assertEquals(self.reader.url, parameters['url'])

  def test_http_api_init(self):
    parameters = {'url': 'http://drunken.guru/', 'username': 'your', 'password': 'mother'}
    self.reader = HttpApi(**parameters)

    self.assertIsNotNone(self.reader)
    self.assertIsInstance(self.reader, HttpApi)
    self.assertEquals(self.reader.url, parameters['url'])
    self.assertEquals(self.reader.username, parameters['username'])
    self.assertEquals(self.reader.password, parameters['password'])

  def test_http_read(self):
    data = {'here': 'is', 'some': 'data'}
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json = lambda : data

    with patch.object(HttpApi, '__init__', return_value=None):
      with patch.object(requests, 'get', return_value=mock_response):
        self.reader = HttpApi()
        self.reader.url = 'http://drunken.guru/'
        self.reader.username = 'your'
        self.reader.password = 'mother'

        self.reader.read()
        self.assertEquals(self.reader.data, data)

  def test_http_read_with_exception(self):
    data = {'here': 'is', 'some': 'data'}
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json = MagicMock(side_effect=ValueError('broke yo'))
    
    with patch.object(HttpApi, '__init__', return_value=None):
      with patch.object(requests, 'get', return_value=mock_response):
        with patch.object(requests.auth, 'HTTPBasicAuth', return_value=None):
          self.reader = HttpApi()
          self.reader.url = 'http://drunken.guru/'
          self.reader.username = 'your'
          self.reader.password = 'mother'

          self.reader.read()
          self.assertEquals(str(self.reader.error), 'broke yo')
          self.assertEquals(self.reader.data, [])
            


  # def test_read(self):
  #   with self.assertRaises(NotImplementedError):
  #     self.reader.read()
  #     self.assertNone(self.reader.data)
  
  # def test_head_data(self):
  #   self.reader.data = [0,1,2,3,4,5,6,7,8,9,10] 
  #   head_result = self.reader.head_data()
  #   self.assertEquals(head_result, self.reader.data[0:9])
  
if __name__ == '__main__':
  run_tests()
