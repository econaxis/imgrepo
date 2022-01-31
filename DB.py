from typing import Optional

from search_lib import CompactingIndexer
from table_manager import TableManager, TableM2

# tbm = TableManager("testfile/test-imgrepo.db")
tbm = TableM2("testfile/test-imgrepo.db")
SEARCH: Optional[CompactingIndexer] = None


def reset_tables():
    global tbm
    tbm.reload()
