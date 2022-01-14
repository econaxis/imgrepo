import io
import json

import flask
from flask import Blueprint, request

import DB
from main import all_pictures
from picture import post_picture, generate_thumbnail, generate_most_similar

api = Blueprint("api", __name__)


@api.route("/post-picture-simple", methods=["POST"])
def store_to_db_simple():
    file_stream = next(request.files.values())
    description = request.form["description"]
    id = post_picture(
        file_stream.read(), description, file_stream.filename, file_stream.mimetype
    )

    return id, 200


@api.route("/img_data")
def img_data():
    id: int = int(request.args["id"])
    result = DB.tbm.get(id)
    if result is None:
        print("Image not found!", id)
        return "Image file not found", 400

    db_data = json.loads(result.description)

    if db_data is None:
        print("Image has been deleted!", id, result)
        return "Image has been deleted", 400

    return flask.send_file(
        io.BytesIO(result.data),
        attachment_filename=db_data["filename"],
        mimetype=db_data["mimetype"],
    )


@api.route("/post-picture", methods=["POST"])
def post_picture_web():
    response = ""
    for file_stream in request.files.getlist("file"):
        file_contents = file_stream.read()
        if len(file_contents) == 0:
            continue
        thumbnail = generate_thumbnail(file_contents)
        tags = generate_most_similar(thumbnail)
        document_str = " ".join(tags)

        print("Picture descr", document_str)
        id = post_picture(
            file_contents, document_str, file_stream.filename, file_stream.mimetype
        )
        response += f"Posted picture {file_stream.filename} to {id}<br><br>"

    if request.form.get("flush"):
        print("auto flushing")
        flush_search()
    return all_pictures()


@api.route("/delete")
def delete_image():
    id = int(request.args["id"])
    print("Deleting image ", id)
    DB.tbm.store(id, "deleted.jpg", "null", b"blank image")
    return flask.redirect(flask.url_for('public.all_pictures'))


@api.route("/flush-search")
def flush_search():
    DB.SEARCH.flush()
    DB.tbm.flush()
    # DB.tbm.compact()
    return "", 200


@api.route("/img_name")
def img_by_name():
    print("Doing img name")
    name = request.args["name"]
    resps = DB.tbm.get_by_name(name)
    urls = []
    for resp in resps:
        urls.append((resp.id, resp.filename))
    return flask.render_template("search-results.html", imgurls=urls)
