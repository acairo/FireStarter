# from pyspark import HiveContext, SparkConf, SparkContext
import requests
from requests.auth import HTTPBasicAuth


class Reader(object):
  # Will everything work coming back as a list to the SC/HC?
  data = []
  
  def read(self):
    self.data = None
    raise NotImplementedError

  def head_data(self, lines=None):
    if not lines:
      lines = 9
    return self.data[0:lines]

  def register_hc_temp(self):
    """Create and register dataframe as temp table for HiveContext.
    How do we decide if the required action is hive or spark and combine functions?"""
    try:
      dataframe = self.hc.createDataFrame(self.data, samplingRatio=1.0)
      self.data = self.hc.registerDataFrameAsTable(dataframe, self.temp_table)
      print "Turned data to dataframe and registered into temp table {0}".format(self.temp_table)
    except Exception as err:
      print "Failed to turn data to dataframe and register as {0}! {1}".format(self.temp_table, err)

  def spark_parallelize_data(self):
    """Create RDD from data for SparkContext.
    Hoping to return this to FireStarter as a distributed RDD for the next action.
    Does this belong in igniter? It's the first step to having a spark sql temp table
    as well as igniting data"""
    try:
      self.data = sc.parallelize(self.data)
      print "Data set parallelized to worker nodes as RDD!"
    except Exception as err:
      print "Failed to parallelize data to worker nodes as RDD! {0}".format(err)


  def spark_infer_schema(self):
    """Infer schema automagically with Spark for later use
    Eploratory function... have any value since its pretty much a one liner that anyone
    in the shell should know?
    """
    #SQLContext needs to be handeled by FireStarter - need to move later
    from pyspark.sql import SQLContext, Row
    sqlContext = SQLContext(sc)
    self.data = sqlContext.inferSchema(self.data)

class HttpApi(Reader):
  """Reads JSON data from remote API endpoint."""

  def __init__(self, **args):
    self.url = args['url']
    self.username = args['username'] if 'username' in args else None
    self.password = args['password'] if 'password' in args else None

  def read(self):
    try:
      if self.username and self.password:
        self.request = requests.get(self.url, verify=False, 
          auth=HTTPBasicAuth(self.username, self.password))
      else:
        self.request = requests.get(self.url, verify=False)

      self.data = self.request.json()
    except ValueError as err:
      # TODO(afammartino): Overwrite NotImplemented log method that stores this
      self.error = err

class ReadHive(Reader):
  """Reads data from Hive table. Use register_hc_temp() to store result in temp."""
  
  def __init__(self, *args):
    #self.hive_db = hive_db - Over simplifying this for now.
    #self.hive_table = hive_table
    #self.temp_table = temp_table
    # self.query = query
    # self.data = self hc.sql(self.query)
    pass

class ReadElasticSearch(Reader):
  """Reads data from remote ES instance.Takes elastic_conf a {} with es.resource and es.nodes..
  Requires the following jar to be imported elasticsearch-hadoop-2.1.2.jar.

   conf = {"es.resource" : "index/type"}   # w/ out conf assume Elasticsearch is running on localhost defaults
   rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat",
     "org.apache.hadoop.io.NullWritable", "org.elasticsearch.hadoop.mr.LinkedMapWritable", conf=conf)
  """

  def __init__(self, *args):
    self.elastic_conf = elastic_conf
  
    es_rdd = sc.newAPIHadoopRDD("org.elasticsearch.hadoop.mr.EsInputFormat", "org.apache.hadoop.io.NullWritable",
      "org.elasticsearch.hadoop.mr.LinkedMapWritable", elastic_conf=conf)
    pass
    #return es_rdd #How to return back to context?
