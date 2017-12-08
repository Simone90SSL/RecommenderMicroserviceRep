import logging
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS

from src.utility_matrix.sparse_utility_matrix import get_utility_matrix_instance

log = logging.getLogger(__name__)


class SparkCF(object):
	def __init__(self, utility_matrix, max_iter=18, rank=100):
		log.info("Initializing the recommender for the utility matrix with name "+utility_matrix.name)
		self.ready = False
		conf = SparkConf() \
			.setAppName("CF") \
			.set("spark.executor.memory", "4g") \
			.set("spark.driver.memory", "4g") \
			.set("spark.cores.max", "10")
		self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
		
		self.utility_matrix_name = utility_matrix.name
		self.sparse_list_matrix, self.user_row_index, self.item_col_index = \
			utility_matrix.get_sparse_list_matrix(boolean=False)
		self.row_user_index = {v: k for k, v in self.user_row_index.items()}
		self.col_item_index = {v: k for k, v in self.item_col_index.items()}
		
		self.max_iter = max_iter
		self.rank = rank
		self.df = self.spark.createDataFrame(self.sparse_list_matrix, ["user", "item", "rating"])
		self.als = ALS(maxIter=self.max_iter, rank=self.rank, regParam=0.01, coldStartStrategy="drop")
		self.model = None
		
		self.batch_user_recommendation = dict()
		self.batch_item_recommendation = dict()
		
	def evaluate_model(self):
		log.info("Evaluating model for utility matrix with name "+self.utility_matrix_name)
		(training, test) = self.df.randomSplit([0.8, 0.2])
		model = self.als.fit(training)
		predictions = model.transform(test)
		evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
		rmse = evaluator.evaluate(predictions)
		log.info("Root-mean-square error = " + str(rmse))
		return rmse
		
	def init_model(self, k=10):
		log.info("fitting model for utility matrix with name "+self.utility_matrix_name)
		self.model = self.als.fit(self.df)
		
		log.info("recommending item to user")
		user_recommendation_rows = self.model.recommendForAllUsers(k) \
			.select("recommendations.item", "recommendations.rating").collect()
		for user_row, row in enumerate(user_recommendation_rows):
			self.batch_user_recommendation[self.row_user_index[user_row]] = \
				[self.col_item_index[item] for item in row.item]
			
		log.info("recommending user to item")
		item_recommendation_cols = self.model.recommendForAllItems(k) \
			.select("recommendations.user", "recommendations.rating").collect()
		for item_col, col in enumerate(item_recommendation_cols):
			self.batch_item_recommendation[self.col_item_index[item_col]] = \
				[self.row_user_index[user] for user in col.user]
		self.ready = True
		
	def get_recommendation_by_user(self, user):
		return self.batch_user_recommendation[user]
			
	def get_recommendation_by_item(self, item):
		return self.batch_item_recommendation[item]
	
	def is_ready(self):
		return self.ready
	
	def __str__(self):
		return "Name: "+self.utility_matrix_name + " Ready: " + str(self.ready)


recommender_dict = dict()

def get_recommender(um = get_utility_matrix_instance()):
	global recommender_dict
	log.info("Get recommmneder for the utility matrix with name: "+um.name)
	try:
		return recommender_dict[um.name]
	except KeyError:
		recommender = SparkCF(um)
		recommender_dict[um.name] = recommender
		recommender.init_model()
	return recommender_dict[um.name]


def is_recommender_ready():
	get_recommender().is_ready()
