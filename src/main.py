import sys
import logging
import os.path

# make the code run under a console
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
project_directory = os.path.dirname(os.path.abspath(__file__))[0:-4]

from src.rest.controller import RestController
from src.spark.spark_cf import get_recommender


def init(log_to_console=True):
	if log_to_console:
		logging.basicConfig(
			level=logging.INFO, format="%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s")
	else:
		logging.basicConfig(
			filename=project_directory + "/log/logging.txt", filemode="w",
			level=logging.INFO, format="%(asctime)s - %(process)d - %(name)s - %(levelname)s - %(message)s")
	
	# start rest controller thread
	t = RestController()
	t.start()
	print("Rest Controller initialized")
	
	# Init the default recommender
	get_recommender()
	print("Recommender initialized")


if __name__ == "__main__":
	init()
	log = logging.getLogger(__name__)
	log.info("APPLICATION STARTED")