import logging
from src.util import util

log = logging.getLogger(__name__)


def export(sparse_user_item, export_directory=None):
	log.info("Exporting Graph")
	
	if not export_directory:
		export_directory = util.get_project_path()+"data/"
	
	with open(export_directory+sparse_user_item.name+ "__users.txt", "w") as f:
		for user in sparse_user_item.users:
			f.write(str(user)+'\n')
		f.close()
	log.info("End exporting user")
	
	with open(export_directory+sparse_user_item.name+ "__items.txt", "w") as f:
		for item in sparse_user_item.items:
			f.write(str(item)+'\n')
		f.close()
	log.info("End exporting items")
	
	with open(export_directory+sparse_user_item.name+ "__user_items.txt", "w") as f:
		for user, user_items in sparse_user_item.user_item.items():
			items_str = ",".join(item + "~" + str(count) for item, count in user_items.items())
			f.write(str(user) + ":" + items_str + '\n')
		f.close()
	log.info("End exporting user-item")
	

def read_from_file(graph, import_directory=None):
	log.info("Importing Graph")
	
	if not import_directory:
		import_directory = util.get_project_path() + "data/"
	
	with open(import_directory+graph.name+"__users.txt", "r") as f:
		user_id = f.readline()
		while user_id:
			graph.add_user(user_id)
			user_id = f.readline()[:-1]
		f.close()
	log.info("End importing user")
	
	with open(import_directory+graph.name+"__items.txt", "r") as f:
		item_id = f.readline()
		while item_id:
			graph.add_item(item_id)
			item_id = f.readline()[:-1]
		f.close()
	log.info("End importing items")
	
	with open(import_directory+graph.name+"__user_items.txt", "r") as f:
		user_item_line = f.readline()[:-1]
		while user_item_line:
			user_id = user_item_line.split(":")[0]
			items_line = user_item_line.split(":")[1]
			for item_count in items_line.split(","):
				graph.add_user_item(user_id, item_count.split("~")[0], item_count.split("~")[1])
			
			user_item_line = f.readline()[:-1]
		f.close()
	log.info("End import user-item")

