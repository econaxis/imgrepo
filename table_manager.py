import codecs
import ctypes
import dataclasses
import functools
import os
from ctypes import Structure, POINTER, c_char_p, c_uint64, c_void_p
from typing import Union


class _TableManager(Structure):
    pass


class Db2Table(Structure):
    pass


class StrFatPtr(ctypes.Structure):
    _fields_ = [("ptr", ctypes.c_void_p), ("len", ctypes.c_uint64)]


@dataclasses.dataclass
class ImageDocument:
    id: int
    filename: str
    description: bytes
    data: bytes


class FFIImageDocument(Structure):
    _fields_ = [
        ("id", c_uint64),
        ("filename", c_void_p),
        ("filename_len", c_uint64),
        ("description", c_void_p),
        ("description_len", c_uint64),
        ("data", c_void_p),
        ("data_len", c_uint64),
    ]

    def to_doc(self):
        a = ImageDocument(
            id=self.id,
            filename=ctypes.string_at(self.filename, self.filename_len).decode("ascii"),
            description=ctypes.string_at(self.description, self.description_len),
            data=ctypes.string_at(self.data, self.data_len),
        )
        assert len(a.data) == self.data_len
        return a


class FFIDocumentArray(Structure):
    _fields_ = [("ptr", POINTER(FFIImageDocument)), ("len", c_uint64)]

    def as_list(self) -> list[ImageDocument]:
        return [x.to_doc() for x in self.ptr[0: self.len]]


def load_rust_lib(path):
    db = ctypes.cdll.LoadLibrary(f"{path}/libdb2.so")
    # db.db1_store.argtypes = [POINTER(_TableManager), c_uint32, c_char_p, c_uint32, c_char_p, c_uint32]
    # db.db1_get.argtypes = [POINTER(_TableManager), c_uint32, c_uint8]
    # db.db1_get.restype = StrFatPtr
    # db.db1_new.restype = POINTER(_TableManager)
    # db.db1_new.argtypes = [c_char_p]
    # db.db1_compact.argtypes = [POINTER(_TableManager), c_char_p]
    db.db2_new.argtypes = [c_char_p]
    db.db2_new.restype = POINTER(Db2Table)
    db.db2_store.argtypes = [
        POINTER(Db2Table),
        c_uint64,
        c_char_p,
        c_uint64,
        c_char_p,
        c_uint64,
        c_char_p,
        c_uint64,
    ]
    db.db2_get.argtypes = [POINTER(Db2Table), c_uint64, ctypes.c_uint8]
    db.db2_get.restype = FFIDocumentArray
    db.db2_get_by_name.argtypes = [POINTER(Db2Table), c_char_p]
    db.db2_get_by_name.restype = FFIDocumentArray
    db.db2_persist.argtypes = [POINTER(Db2Table)]

    db.db2_get_all.restype = FFIDocumentArray
    return db


DBDLL = load_rust_lib("./")


class TableManager:
    def __init__(self, path="/tmp/test.db"):
        self.tbm = DBDLL.db2_new(path.encode("ascii"))
        self.path = path

    def store(
            self,
            id: int,
            name: Union[bytes, str],
            description: Union[bytes, str],
            data: Union[bytes, str],
    ):
        def to_bytes(a):
            if type(a) != bytes:
                orig_len = len(a)
                a = codecs.encode(a, "utf8", "replace")
                assert orig_len == len(a)
            return a

        name = to_bytes(name)
        description = to_bytes(description)
        data = to_bytes(data)

        DBDLL.db2_store(
            self.tbm,
            id,
            name,
            len(name),
            description,
            len(description),
            data,
            len(data),
        )

    def copy_bytes(self, a: StrFatPtr):
        if a.ptr:
            return ctypes.string_at(a.ptr, a.len)[:]
        else:
            return None

    def get(self, id: int, mask: int = 255) -> ImageDocument:
        assert mask <= 255
        im = DBDLL.db2_get(self.tbm, id, mask)
        return im.as_list()[0]

    def get_by_name(self, name: str) -> list[ImageDocument]:
        return DBDLL.db2_get_by_name(self.tbm, name.encode("ascii")).as_list()

    def flush(self):
        DBDLL.db2_persist(self.tbm)

    def load_first_n(self, n: int):
        li: list[ImageDocument] = DBDLL.db2_get_all(self.tbm, 1).as_list()
        results = []
        for i in li[0:n]:
            results.append(self.get(i.id, 0b111))
        return results

    def reload(self):
        self.flush()
        DBDLL.db2_drop(self.tbm)
        self.__init__(self.path)

    def compact(self):
        raise RuntimeError
        temp_path = b"temp-db-path"
        DBDLL.db1_compact(self.tbm, temp_path)

        os.rename(temp_path, self.path)

        self.__init__(self.path)


def test_tbm():
    try:
        os.remove("/tmp/test_tbm")
    except Exception as e:
        pass
    db = TableManager("/tmp/test_tbm")
    db.store(12, "hello.jpg", "hlfdsafdsa", "fjdlksa; jlkavcx")
    db.store(13, "hello.jpg", "hlfdsafdsa", "fjdlksa; jlkavcx")
    db.store(14, "hello.jpg", "hlfdsafdsa", "fjdlksa; jlkavcx")
    print(db.get_by_name("hello.jpg"))
    print(db.get(13))
