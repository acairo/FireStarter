class SparkConf(object):
  def __init__(self):
    print "inside SparkConf"
    self.attrs = {}
  def setAppName(self,blah):
    print "inside SparkConf"
    self.attrs['app_name'] = blah
  def set(self,blah,blahh):
    print "inside SparkConf"
    self.attrs[blah] = blahh

class SparkContext():
  def __init__(self, conf = None):
    print "hello"
    self.conf = conf