import asyncio
import os
import time
import zlib
from datetime import datetime
from os import read
import gzip
import aiofiles
import ijson
from orjson import orjson
import psycopg2
from psycopg2.extras import execute_values

CHUNKSIZE = 4096
d = zlib.decompressobj(16+zlib.MAX_WBITS)


class Uploader:

    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=5432,
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            database=os.environ["DB_DATABASE"],
        )
        self.cursor = self.conn.cursor()
        self.bt = datetime.now()
        self.et = None
        self.last_time = datetime.now()
        self.buff: dict[str, list] = {
            "authors": [],
            "conversations": [],
            "hashtags": [],
            "conversation_hashtags": [],
            "context_domains": [],
            "context_entities": [],
            "context_annotations": [],
            "annotations": [],
            "links": [],
            "conversation_references": [],
        }

    def upload(self, *args):
        raise NotImplementedError

    def close(self):
        self.et = datetime.now()
        print("Total time elapsed: {}".format(self.et - self.bt))

    def _do_upload(self, table_name, force=False):
        if len(self.buff[table_name]) % 10000 == 0 or force:
            print("{},{},{}".format(
                datetime.now().isoformat(),
                datetime.now() - self.bt,
                datetime.now() - self.last_time))
            self.last_time = datetime.now()
            execute_values(self.cursor, "INSERT INTO {} VALUES %s ON CONFLICT DO NOTHING".format(table_name), self.buff[table_name])
            self.conn.commit()
            self.buff[table_name] = []


class AuthorsUploader(Uploader):

    def upload(self, json_data: dict):
        self.buff["authors"].append((
            json_data["id"], json_data["name"], json_data["username"], json_data["description"],
            json_data["public_metrics"]["followers_count"], json_data["public_metrics"]["following_count"],
            json_data["public_metrics"]["tweet_count"], json_data["public_metrics"]["listed_count"]
        ))
        self._do_upload("authors")

    def close(self):
        if len(self.buff["authors"]) > 0:
            self._do_upload("authors", force=True)
        super().close()


class ConversationsUploader(Uploader):

    def upload(self, json_data: dict):
        self.buff["context_domains"].append((

        ))


        self._do_upload("authors")


    def __upload_context_annotations(self, json_data: dict):
        # annotations
        for item in json_data["entities"].get("annotations", []):
            self.buff["links"].append((
                None, json_data["conversation_id"], item["normalized_text"], item["type"], item["probability"]
            ))
            self._do_upload("annotations")

    def __upload_annotations(self, json_data: dict):
        # annotations
        for item in json_data["entities"].get("annotations", []):
            self.buff["links"].append((
                None, json_data["conversation_id"], item["normalized_text"], item["type"], item["probability"]
            ))
            self._do_upload("annotations")


    def __upload_links(self, json_data: dict):
        # links
        for item in json_data["entities"].get("urls", []):
            self.buff["links"].append((
                None, json_data["conversation_id"], item["expanded_url"], item["title"], item["description"]
            ))
            self._do_upload("links")

    def __upload_references(self, json_data: dict):
        # conversation_references
        for item in json_data.get("referenced_tweets", []):
            self.buff["conversation_references"].append((
                json_data["conversation_id"], item["id"], item["type"]
            ))
        self._do_upload("conversation_references")

    def close(self):
        if len(self.buff["authors"]) > 0:
            self._do_upload("authors", force=True)
        super().close()

def main():
    """
    uploader = AuthorsUploader()
    with gzip.open("D:\\Downloads\\authors.jsonl.gz", 'rt') as f:
        for line in f:
            json_data = orjson.loads(line.replace("\\u0000", ""))  # need to replace this because postgres wont accept \x00 cahracter
            uploader.upload(json_data)
    uploader.close()
    """
    uploader = ConversationsUploader()
    with gzip.open("D:\\Downloads\\conversations.jsonl.gz", 'rt') as f:
        for line in f:
            json_data = orjson.loads(line.replace("\\u0000", ""))  # need to replace this because postgres wont accept \x00 cahracter
            #uploader.upload(json_data)
            print(json_data)
            break
    uploader.close()


if __name__ == '__main__':
    main()
