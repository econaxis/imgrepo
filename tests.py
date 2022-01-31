import io
import random

import flask

import DB
from api import flush_search


def fill_with_images(app):
    with app.test_client() as client:
        for i in range(0, 3):
            response: flask.Response = client.post(
                "/post-picture",
                data={
                    "file": (
                        open(f"/home/henry/Downloads/sample{i}.jpg", "rb"),
                        f"sample{i}.jpg",
                    ),
                },
                content_type="multipart/form-data",
            )
            assert response.status_code == 200
    DB.tbm.flush()
    flush_search()


def overall_test(app):
    for _ in range(0, 4):
        with app.test_client() as client:
            for i in range(0, 8):
                response: flask.Response = client.post(
                    "/post-picture",
                    data={
                        "file": (
                            open(f"/home/henry/Downloads/sample{i}.jpg", "rb"),
                            f"sample{i}.jpg",
                        ),
                    },
                    content_type="multipart/form-data",
                )
                assert response.status_code == 200
            client.get("/flush-search")
            client.get("/search-picture", query_string={"query": "snow"})
            client.get("/search-picture", query_string={"query": "family"})

        DB.tbm.flush()


def post_image_test(app):
    with app.test_client() as client:
        stored = {}
        for _ in range(0, 1000):
            content = random.randbytes(2000)
            id = int(
                client.post(
                    "/post-picture-simple",
                    data={
                        "file": (io.BytesIO(content), "test.jpg"),
                        "description": "test image",
                    },
                    content_type="multipart/form-data",
                ).data
            )
            stored[id] = content

        # DB.tbm.flush()
        print("FIRST N", DB.tbm.load_first_n(10))
        for id, content in stored.items():
            received = client.get("/img_data", query_string={"id": id}).data
            assert (
                    received == content
            ), f"Received {received.decode('ascii', errors='ignore')} Content {content.decode('ascii', errors='ignore')}"


def search_image_test(app):
    with app.test_client() as client:
        id_alpine = int(
            client.post(
                "/post-picture-simple",
                data={
                    "file": (io.BytesIO(b"aaaaa"), "test.jpg"),
                    "description": "this file contains the word alpine",
                },
                content_type="multipart/form-data",
            ).data
        )
        id_watermelon = int(
            client.post(
                "/post-picture-simple",
                data={
                    "file": (io.BytesIO(b"aaaaa"), "test.jpg"),
                    "description": "this file contains the word watermelon",
                },
                content_type="multipart/form-data",
            ).data
        )

        assert client.get("/flush-search").status_code == 200
        print(DB.SEARCH.main)
        assert [id_alpine] == list(DB.SEARCH.search_terms("ALPINE").scores.keys())
        assert [id_watermelon] == list(
            DB.SEARCH.search_terms("WATERMELON").scores.keys()
        )


def test_delete_image(app):
    with app.test_client() as client:
        deleted_id = client.post(
            "/post-picture-simple",
            data={
                "file": (io.BytesIO(b"fdsafdafdsa8f 0"), "test.jpg"),
                "description": "image should be deleted soon",
            },
        ).data

        keep_id = client.post(
            "/post-picture-simple",
            data={
                "file": (io.BytesIO(b"kept image"), "test.jpg"),
                "description": "image should be kept",
            },
        ).data

        client.get(
            flask.url_for("api.delete_image"), query_string={"id": deleted_id}
        )

        deleted_response = client.get(
            flask.url_for("api.img_data"), query_string={"id": deleted_id}
        )
        # Should return an error
        assert deleted_response.status_code == 400

        kept_response = client.get(
            flask.url_for("api.img_data"), query_string={"id": keep_id}
        )
        # Should return an error
        assert kept_response.status_code == 200
        assert kept_response.data == b"kept image"


def test_persistence(app):
    with app.test_client() as client:
        print("Starting persistence test")
        id = client.post(
            "/post-picture-simple",
            data={
                "file": (io.BytesIO(b"test persistence image data"), "test.jpg"),
                "description": "a a aa ",
            },
        ).data

        client.get("/flush-search")

        DB.tbm.reload()
        assert (
                client.get(flask.url_for("api.img_data"), query_string={"id": id}).data
                == b"test persistence image data"
        )

        DB.tbm.reload()
        assert (
                client.get(flask.url_for("api.img_data"), query_string={"id": id}).data
                == b"test persistence image data"
        )
