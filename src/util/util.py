import os
import logging
import multiprocessing as mp


log = logging.getLogger(__name__)

def get_project_path():
	return os.path.dirname(os.path.abspath(__file__))[0:-8]
	
	
def raise_exception(msg):
	raise Exception(msg)


def timeout(func, args=(), kwds={}, timeout=3, default = None):
	pool = mp.Pool(processes=1)
	result = pool.apply_async(func, args = args, kwds = kwds)
	try:
		val = result.get(timeout=timeout)
	except mp.TimeoutError:
		pool.terminate()
		raise Exception("Time out error")
	else:
		pool.close()
		pool.join()
		return val


def push_recommendation(recommender, graph_http_client):
	log.info("Start pushing recommendation")
	for user, item_list in recommender.batch_user_recommendation.items():
		log.info("Recommend item for user "+user)
		graph_http_client.post_recommendation(user, item_list)
	log.info("Finished to recommend")
