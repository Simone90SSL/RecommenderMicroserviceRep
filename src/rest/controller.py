from threading import Thread
import logging
import json
from flask import Flask
from flask import render_template

from src.util import util
from src.utility_matrix.sparse_utility_matrix import get_utility_matrix_instance
from src.spark.spark_cf import get_recommender, is_recommender_ready
from src.rest.graph_client import GraphHttpClient

log = logging.getLogger(__name__)

app = Flask(__name__)


@app.route('/')
def index():
	log.info("REQUEST '/' -> Recommender Micorservice: HOME")
	return render_template('index.html')


@app.route('/utilitymatrix')
def utility_matrix_home():
	log.info("REQUEST '/utilitymatrix' -> Utility Matrix: HOME")
	return render_template('utilitymatrix.html')


@app.route('/utilitymatrix/refresh')
def utility_matrix_refresh():
	log.info("REQUEST '/utilitymatrix/refresh'")
	sparse_utility_matrix = get_utility_matrix_instance()
	try:
		sparse_utility_matrix.read(stream=True)
		sparse_utility_matrix.export()
	except ConnectionRefusedError:
		log.warning("Connection refused from Social Network Microservice")
	return sparse_utility_matrix.name+" utility matrix correctly refreshed"


@app.route('/utilitymatrix/read')
def utility_matrix_read():
	log.info("REQUEST '/utilitymatrix/read'")
	sparse_utility_matrix = get_utility_matrix_instance()
	sparse_utility_matrix.read(stream=False)
	return sparse_utility_matrix.name+" utility matrix correctly read"


@app.route('/utilitymatrix/export')
def utility_matrix_export():
	log.info("REQUEST '/utilitymatrix/export'")
	sparse_utility_matrix = get_utility_matrix_instance()
	sparse_utility_matrix.export()
	return sparse_utility_matrix.name+" utility matrix correctly exported"


@app.route('/recommender')
def recommender_home():
	log.info("REQUEST '/recommender' -> Recommender: HOME")
	try:
		util.timeout(is_recommender_ready, timeout=3)
		if get_recommender().is_ready():
			status = "READY"
		else:
			status = "NOT READY"
	except Exception as e:
		print(e)
		status = "NOT READY"
	return render_template('recommender.html', status=status)


@app.route('/recommender/push')
def recommender_push():
	recommender = get_recommender()
	if recommender.is_ready():
		graph_http_client = GraphHttpClient()
		thread = Thread(target=util.push_recommendation, args=(recommender, graph_http_client, ))
		thread.start()
		return "PUSHING RECOMMENDATION: check the logs"
	else:
		return "RECOMMENDER IS NOT READY"


@app.route('/recommender/user/<string:user>', methods=['GET'])
def recommender_user(user):
	recommender = get_recommender()
	utility_matrix = get_utility_matrix_instance()
	try:
		response = dict()
		response["user"] = user
		response["item_seen"] = ','.join(utility_matrix.user_item[user].keys())
		response["recommendation"] = ','.join(recommender.get_recommendation_by_user(user))
		return json.dumps(response)
	except KeyError:
		return "User Not Found"


@app.route('/recommender/item/<string:item>', methods=['GET'])
def recommender_tag(item):
	recommender = get_recommender()
	utility_matrix = get_utility_matrix_instance()
	try:
		response = dict()
		response["item"] = item
		response["user_seen"] = ','.join(utility_matrix.item_user[item].keys())
		response["recommendation"] = ','.join(recommender.get_recommendation_by_item(item))
		return json.dumps(response)
	except KeyError:
		return "Item Not Found"


class RestController(Thread):
	def __init__(self,):
		Thread.__init__(self)
	
	def run(self):
		log.info("Starting rest controller")
		app.run(debug=False, port=2003)