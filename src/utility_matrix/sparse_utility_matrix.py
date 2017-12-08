import logging
import time

from src.utility_matrix import utility_matrix_util
from src.rest.graph_client import GraphHttpClient

log = logging.getLogger(__name__)


class SparseUtilityMatrix(object):
	
	def __init__(self, name="DEFAULT"):
		self.name = name
		
		self.users = set()
		self.items = set()
		self.following = dict()
		self.user_item = dict()
		self.item_user = dict()
		self.user_user = dict()
		self.item_item = dict()
		
		self.n_users = 0
		self.n_items = 0
		
	def add_user(self, user_id):
		self.users.add(user_id)
		self.n_users = len(self.users)
		
	def add_item(self, item_id):
		self.items.add(item_id)
		self.n_items = len(self.items)
	
	def add_user_item(self, user, item, count):
		try:
			self.user_item[user][item] = count
		except KeyError:
			self.user_item[user] = dict()
			self.user_item[user][item] = count
			
		try:
			self.item_user[item][user] = count
		except KeyError:
			self.item_user[item] = dict()
			self.item_user[item][user] = count
			
	def get_sparse_row_matrix(self):
		user_col_index = dict()
		item_row_index = dict()
		user_col_count = 0
		item_row_count = 0
		sparse_rows = []
		for item, user_count in self.item_user.items():
			item_row_index[item] = item_row_count
			item_row_count += 1
			if item_row_count % 1000 is 0:
				log.info(
					"Get sparse row matrix representation at "
					+ str(round(item_row_count/len(self.item_user) * 100, 2)) + "%")
			sparse_row = dict()
			for user, count in user_count.items():
				try:
					user_col = user_col_index[user]
				except KeyError:
					user_col = user_col_count
					user_col_index[user] = user_col_count
					user_col_count += 1
				sparse_row[user_col] = int(count)
			sparse_rows.append(sparse_row)
		
		return sparse_rows, user_col_index, item_row_index
	
	def get_sparse_list_matrix(self, boolean=True):
		user_row_index = dict()
		item_col_index = dict()
		user_row_count = 0
		item_col_count = 0
		sparse_list_matrix = []
		for user, item_count in self.user_item.items():
			user_row_index[user] = user_row_count
			user_row_count += 1
			if user_row_count % 1000 is 0:
				log.info(
					"Get sparse list matrix representation at "
					+ str(round(user_row_count / len(self.user_item) * 100, 2)) + "%")
			for item, count in item_count.items():
				try:
					item_col = item_col_index[item]
				except KeyError:
					item_col = item_col_count
					item_col_index[item] = item_col_count
					item_col_count += 1
				if boolean:
					count = 1
				component_tuple = (user_row_index[user], item_col, int(count))
				sparse_list_matrix.append(component_tuple)
		
		return sparse_list_matrix, user_row_index, item_col_index
		
	def read(self, stream=False):
		log.info("Reading graph")
		self.__init__(name=self.name)
		start = time.time()
		if stream:
			graphClient = GraphHttpClient()
			graphClient.get_users(self)
			graphClient.get_items(self)
			graphClient.get_user_item(self)
		else:
			utility_matrix_util.read_from_file(self)
		
		self.read_user_user()
		self.read_item_item()
		end = time.time()
		log.info("Read the graph in %d seconds", end-start)
		
	def export(self):
		utility_matrix_util.export(self)
	
	def read_user_user(self):
		log.info("Reading User-Users")
		for item, users in self.item_user.items():
			for user1 in users:
				for user2 in users:
					if user1 < user2:
						try:
							self.user_user[user1].add(user2)
						except KeyError:
							self.user_user[user1] = set()
							self.user_user[user1].add(user2)
						try:
							self.user_user[user2].add(user1)
						except KeyError:
							self.user_user[user2] = set()
							self.user_user[user2].add(user1)
	
	def read_item_item(self):
		log.info("Reading Item-Items")
		for user, items in self.user_item.items():
			for item1 in items:
				for item2 in items:
					if item1 < item2:
						try:
							self.item_item[item1].add(item2)
						except KeyError:
							self.item_item[item1] = set()
							self.item_item[item1].add(item2)
						
						try:
							self.item_item[item2].add(item1)
						except KeyError:
							self.item_item[item2] = set()
							self.item_item[item2].add(item1)


sparse_utility_matrix_dict = dict()


def get_utility_matrix_instance(name="DEFAULT"):
	try:
		sparse_utility_matrix_dict[name]
	except KeyError:
		sparse_utility_matrix_dict[name] = SparseUtilityMatrix(name=name)
		sparse_utility_matrix_dict[name].read(stream=False)
	
	return sparse_utility_matrix_dict[name]
