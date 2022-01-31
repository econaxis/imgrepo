import io
import json

from PIL import Image
from azure.cognitiveservices.vision.computervision import ComputerVisionClient
from gensim.models import KeyedVectors
from msrest.authentication import CognitiveServicesCredentials

import DB
from creds import ENDPOINT, KEY1 as SUBSCRIPTION_KEY

computervision_client = ComputerVisionClient(
    ENDPOINT, CognitiveServicesCredentials(SUBSCRIPTION_KEY)
)

# MODEL = KeyedVectors.load_word2vec_format("glove.6B.100d.txt", no_header=True)
# MODEL.save("glove.bin")
MODEL = KeyedVectors.load("glove.bin")


def generate_most_similar(img) -> [str]:
    tags = [t.name for t in computervision_client.tag_image_in_stream(img).tags]
    tags_knn = list(filter(lambda k: MODEL.has_index_for(k), tags))
    synonyms = MODEL.most_similar(positive=tags_knn, topn=80)
    return [x[0] for x in synonyms] + tags


def generate_thumbnail(img):
    with Image.open(io.BytesIO(img)).convert("RGB") as im:
        im.thumbnail((512, 512))
        out = io.BytesIO()
        im.save(out, format="JPEG")
        out.seek(0)
        return out


def post_picture(file, description, filename, mimetype):
    id = DB.SEARCH.append_file(description)
    print("Posting to ", id)

    db_data = dict(synonyms_desc=description, filename=filename, mimetype=mimetype)
    DB.tbm.store(id, filename, json.dumps(db_data), file)
    return str(id)
