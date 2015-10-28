
class Reader(object):
  
  def read(self):
    self.data = None
    raise NotImplementedError

  def head_data(self, lines=None):
    if not lines:
      lines = 9
    return self.data[0:lines]


class HttpApi(Reader):
  
  def __init__(self, *args):
    pass
