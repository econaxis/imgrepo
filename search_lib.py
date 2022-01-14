import codecs
import ctypes
import json
import urllib.parse
from ctypes import POINTER, c_char_p, c_uint32, c_uint8, Structure, c_bool
from queue import Queue
from threading import Thread
from typing import Iterable, Optional


class SortedKeysIndexStub(Structure):
    pass


class SortedKeysIndex(Structure):
    pass


class DocumentFrequency(Structure):
    _fields_ = [("document_id", c_uint32), ("document_freq", c_uint32)]


class FoundPositions(Structure):
    _fields_ = [
        ("terms_index", c_uint8),
        ("document_id", c_uint32),
        ("document_position", c_uint32),
    ]

    def __rich__(self):
        return f"{self.terms_index + 1}th term {self.document_id} @ {self.document_position}"


def limit5(iterator):
    limit = 5
    while limit > 0:
        limit -= 1
        yield next(iterator)


class _SearchRetType(Structure):
    _fields_ = [
        ("topdocs", POINTER(DocumentFrequency)),
        ("topdocs_length", c_uint32),
        ("pos", POINTER(FoundPositions)),
        ("pos_len", c_uint32),
    ]

    def iter_positions(self) -> Iterable[POINTER(FoundPositions)]:
        for i in range(0, self.pos_len):
            yield self.pos[i]

    def iter_td(self, limit=40) -> Iterable[POINTER(DocumentFrequency)]:
        for i in list(reversed(range(0, self.topdocs_length)))[0:limit]:
            yield self.topdocs[i]


class SearchRetType:
    def __init__(self, dll, sr: _SearchRetType, terms: [bytes]):
        td = {}
        scores = {}
        td_terms_count = {}
        for i in sr.iter_td():
            td[int(i.document_id)] = []
            scores[int(i.document_id)] = i.document_freq
            td_terms_count[int(i.document_id)] = [0] * len(terms)

        for i in sr.iter_positions():
            id = i.document_id
            if id not in td:
                continue

            td[id].append([i.document_position, i.terms_index])

        # for i in reversed(sorted(td, key=lambda i: scores[i])):
        #     td[i] = sorted(td[i])
        #     if len(td[i]) == 0:
        #         continue
        #     new = [td[i][0]]
        #     for pos, length in td[i][1:]:
        #         if pos - new[-1][0] - new[-1][1] < 30:
        #             new[-1][1] = length + pos - new[-1][0]
        #         else:
        #             new.append([pos, length])

        dll.free_elem_buf(sr)
        # scores = list(reversed(sorted(scores.keys(), key=lambda k: scores[k])))
        self.matches = td
        self.scores = scores

    def printable(self):
        return json.dumps(dict(matches=self.matches, scores=self.scores))


def load(path):
    indexer = ctypes.cdll.LoadLibrary(f"{path}/libgeneral-indexer.so")
    indexer.initialize_directory_variables.argtypes = [c_char_p]
    indexer.initialize_directory_variables(b"testfile")
    indexer.new_index.argtypes = []
    indexer.new_index.restype = POINTER(SortedKeysIndex)

    indexer.append_file.argtypes = [POINTER(SortedKeysIndex), c_char_p, c_uint32]
    indexer.append_file.restype = None

    indexer.persist_indices.argtypes = [POINTER(SortedKeysIndex), c_char_p]

    indexer.search_many_terms.argtypes = [
        POINTER(SortedKeysIndexStub),
        POINTER(c_char_p),
        c_uint32,
        c_bool,
        c_bool,
    ]

    indexer.search_many_terms.restype = _SearchRetType

    indexer.create_index_stub.restype = POINTER(SortedKeysIndexStub)
    indexer.clean.argtypes = [POINTER(SortedKeysIndex)]
    return indexer


DLL = load(".")


class Searcher:
    def __init__(self, suffix: str):
        self.dll = DLL
        self.suffix = suffix
        print("Loading search ", suffix)
        self.ind = self.dll.create_index_stub(bytes(suffix, "ascii"))

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.dll.free_index(self.ind)

    def search_terms(self, *args) -> SearchRetType:
        terms_len = len(args)

        args = list(map(lambda k: bytes(k, "ascii"), args))
        terms = (c_char_p * terms_len)(*args)

        terms = ctypes.cast(terms, POINTER(c_char_p))

        result = self.dll.search_many_terms(self.ind, terms, terms_len, True, True)

        return SearchRetType(self.dll, result, terms[0:terms_len])


class Indexer:
    def __init__(self, starting_offset=0):
        self.dll = DLL
        self.ind = self.dll.new_index()

    def __enter__(self):
        return self

    def append_file(self, contents: str, id: int) -> int:
        if type(contents) != bytes:
            orig_len = len(contents)
            contents = codecs.encode(contents, "ascii", "replace")
            assert orig_len == len(contents)
        self.dll.append_file(self.ind, contents, id)

    def persist(self, suffix: str):
        self.dll.persist_indices(self.ind, bytes(suffix, "ascii"))

    def concat(self, other):
        self.dll.concat_indices(self.ind, other.ind)
        other.ind = None
        other.dll = None

    def clean(self):
        self.dll.clean(self.ind)

    def address(self):
        return self.ind

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class ParallelIndexer:
    def thread_run(self, index: Indexer):
        def inner():
            while True:
                item = self.queue.get()
                if item == "exit":
                    index.clean()
                    return

                index.append_file(item[0], item[1])

        return inner

    def __init__(self, num_t=15, name="par-index"):
        self.num_t = num_t
        self.name = name
        self.count = 0

    def __enter__(self):
        self.queue = Queue(50)
        self.indices = []
        for _ in range(0, self.num_t):
            ind = Indexer()
            ind.__enter__()
            self.indices.append(ind)

        self.threads = [Thread(target=self.thread_run(i)) for i in self.indices]
        [x.start() for x in self.threads]
        return self

    def append_file(self, contents: str, id: int):
        self.queue.put((contents, id))
        self.count += 1
        if self.count % 500 == 0:
            print(self.count)

    def end(self) -> str:
        for _ in self.threads:
            self.queue.put("exit")
            self.queue.put("exit")

        for t in self.threads:
            t.join()

        print("Merging")
        for t in self.indices[1:]:
            print("Merging")
            self.indices[0].concat(t)

        self.indices[0].persist(self.name)
        return self.name

    def __exit__(self, exc_type, exc_val, exc_tb):
        [i.__exit__(exc_type, exc_val, exc_tb) for i in self.indices]


COMPACTORDLL = ctypes.cdll.LoadLibrary(
    "./libcompactor.so"
)
COMPACTORDLL.initialize_directory_variables(b"testfile")
COMPACTORDLL.compact_two_files.argtypes = [c_char_p, c_char_p, c_char_p]


class Compactor:
    @classmethod
    def compact_files(cls, a: str, b: str, out: str):
        assert a != b
        COMPACTORDLL.compact_two_files(
            a.encode("ascii"), b.encode("ascii"), out.encode("ascii")
        )


class CompactingIndexer:
    main: Optional[Searcher]
    count: int

    def __init__(self, start_key, start_prefix):
        print("Allocating new searcher")
        if start_prefix:
            self.main = Searcher(start_prefix)
        else:
            self.main = None
        self.count = start_key
        self.working = Indexer(self.count)

    def search_terms(self, terms: str) -> SearchRetType:
        terms = urllib.parse.unquote_plus(terms).upper().split(" ")

        return self.main.search_terms(*terms)

    def append_file(self, file):
        self.count += 1
        self.working.append_file(file, self.count)
        return self.count

    def id_count(self):
        return self.count

    def name(self):
        return self.main.suffix

    def flush(self):
        print("Flushing")
        new_suffix = "temp-index" if self.main else "main"
        self.working.persist(new_suffix)
        self.working.__exit__(None, None, None)
        self.working = Indexer(self.count)

        if self.main is None:
            self.main = Searcher(new_suffix)
        else:
            self.main.__exit__(None, None, None)
            Compactor.compact_files(self.main.suffix, new_suffix, "main")
            self.main = Searcher("main")


def test():
    for ind, suffix in enumerate(["a", "b"]):
        indexer = Indexer(ind * 2)
        indexer.append_file("hello world")
        indexer.append_file("fajds f08ad f0sa8df saf;dsaf l;fj lkfjdsal kf")
        indexer.persist(suffix)
    Compactor.compact_files(b"a", b"b", b"ab")

    s = Searcher("ab")
    print(s.search_terms("LKFJ").printable())
    exit(0)
