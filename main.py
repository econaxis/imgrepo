import json

import flask
from flask import request

import DB

public = flask.Blueprint("public", __name__)


@public.route("/upload")
def upload():
    return flask.render_template("uploadform.html")


@public.route("/picture")
def picture_by_id():
    id: int = int(request.args["id"])
    url = flask.url_for("api.img_data", id=id)
    db_data = DB.tbm.get_name(id)

    if db_data is None:
        raise RuntimeError("ID not found!")
    db_data = json.loads(db_data)
    title = db_data["filename"]
    return flask.render_template("picture.html", title=title, url=url)


@public.route("/all")
def all_pictures():
    images = DB.tbm.load_first_n(20)
    imgurls = []
    for i in images:
        if i.description == b"null":
            continue

        imgurls.append((i.id, i.filename))
    return flask.render_template("search-results.html", imgurls=imgurls)


@public.route("/search")
def search():
    return flask.render_template("search.html")


@public.route("/search-results")
def search_picture():
    search_query: str = request.args["query"]
    a = DB.SEARCH.search_terms(search_query)
    print(a.printable())
    imgurls = []
    for id in a.scores.keys():
        if a.scores[id] < 4:
            continue
        file = DB.tbm.get(id, 0b0111)
        if not json.loads(file.description):
            continue
        matched_words = []
        for match, mlen in a.matches[id]:
                matched_words.append(
                    json.loads(file.description)["synonyms_desc"][match: match + mlen]
                )

        caption_string = f"<b>{file.filename}</b><br>Matched {','.join(matched_words)}"
        imgurls.append((id, caption_string))
    return flask.render_template("search-results.html", imgurls=imgurls)


@public.route("/img-view")
def image_view():
    id = int(request.args["id"])
    data = DB.tbm.get(id, 0b0111)
    return flask.render_template("picture.html", id=id, filename=data.filename,
                                 description=data.description)
