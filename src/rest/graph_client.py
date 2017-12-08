import http.client
import requests
import logging
import time

log = logging.getLogger(__name__)


class GraphHttpClient(object):
	
	def __init__(self, host="localhost", port=2002):
		self.host = host
		self.port = port
		self.url = "http://"+self.host+":"+str(self.port)
		self.conn = http.client.HTTPConnection(host+":"+str(port))
		
	def get_users(self, graph):
		log.info("Getting users from stream http")
		start_time = time.time()
		self.conn.request("GET", self.url+"/stream/user")
		r = self.conn.getresponse()
		user_id = r.readline().decode('utf-8')[:-1]
		while user_id:
			graph.add_user(user_id)
			user_id = r.readline().decode('utf-8')[:-1]
		execution_time = time.time() - start_time
		log.info("loaded "+str(graph.n_users)+" users in "+str(round(execution_time, 2))+" seconds")
		log.info("User throughput: "+str(round(graph.n_users / execution_time, 2))+" users/sec")
	
	def get_items(self, graph):
		log.info("Getting items from stream http")
		start_time = time.time()
		self.conn.request("GET", self.url + "/stream/hashTag")
		r = self.conn.getresponse()
		item_id = r.readline().decode('utf-8')[:-1]
		while item_id:
			graph.add_item(item_id)
			item_id = r.readline().decode('utf-8')[:-1]
		
		execution_time = time.time() - start_time
		log.info("loaded " + str(graph.n_items) + " items in " + str(execution_time) + " seconds")
		log.info("Items throughput: "+str(round(graph.n_items / execution_time, 2))+" items/sec")
	
	def get_user_item(self, graph):
		log.info("Getting user item pair from stream http")
		start_time = time.time()
		self.conn.request("GET", self.url + "/stream/userTag")
		r = self.conn.getresponse()
		user_item = r.readline().decode('utf-8')[:-1]
		c = 0
		while user_item:
			c += 1
			user = user_item.split(",")[0]
			item = user_item.split(",")[1]
			count = user_item.split(",")[2]
			graph.add_user_item(user, item, count)
			user_item = r.readline().decode('utf-8')[:-1]
		
		execution_time = time.time() - start_time
		log.info("loaded " + str(c) + " user item pairs in " + str(execution_time) + " seconds")
		log.info("UserItems throughput: "+str(round(graph.n_items / execution_time, 2))+" pairs/sec")
	
	def post_recommendation(self, user, items):
		log.info("Posting a recommendation for user "+user)
		items_str = ','.join(items)
		r = requests.post(self.url + "/recommendation/user/"+user, items_str)
		if r.status_code != 200:
			log.warning("Server Error: " + r.text)