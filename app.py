import os

from flask import Flask

import DB
import tests
from api import api
from main import public
from search_lib import CompactingIndexer


def remove_files_from_previous():
    try:
        os.remove("testfile/test-imgrepo.db")
        os.remove("testfile/frequencies-main")
        os.remove("testfile/terms-main")
        os.remove("testfile/positions-main")
    except FileNotFoundError:
        pass


def init_search():
    max_key = len(DB.tbm.exec_sql("SELECT * FROM images"))
    print("Max key: ", max_key)
    if os.path.exists("testfile/frequencies-main"):
        start_prefix = "main"
    else:
        start_prefix = None
    DB.SEARCH = CompactingIndexer(max_key, start_prefix)
    DB.reset_tables()


def reset():
    remove_files_from_previous()
    init_search()
    DB.reset_tables()


app = Flask(__name__)
app.config["TESTING"] = True
app.config["PROPAGATE_EXCEPTIONS"] = True
app.register_blueprint(public, url_prefix="/")
app.register_blueprint(api)
#reset()
#tests.search_image_test(app)
#reset()
#tests.post_image_test(app)
#reset()
## tests.overall_test(app)
## reset()
#tests.test_delete_image(app)
#reset()
#tests.test_persistence(app)
#
# init_search()
# tests.fill_with_images(app)
init_search()
app.run(debug=False, threaded=False)
