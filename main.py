import os
from datetime import datetime
import gzip
from orjson import orjson
import psycopg2
from psycopg2.extras import execute_values


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
        self.templates: dict[str, str] = {
            "authors": "(id, name, username, description, followers_count, following_count, tweet_count, listed_count)",
            "conversations": "(id, author_id, content, possibly_sensitive, language, source, retweet_count, reply_count, like_count, quote_count, created_at)",
            "hashtags": "(tag)",
            "conversation_hashtags": "(conversation_id, hashtag_id)",
            "context_domains": "(id, name, description)",
            "context_entities": "(id, name, description)",
            "context_annotations": "(conversation_id, context_domain_id, context_entity_id)",
            "annotations": "(conversation_id, value, type, probability)",
            "links": "(conversation_id, url, title, description)",
            "conversation_references": "(conversation_id, parent_id, type)",
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
            execute_values(self.cursor, "INSERT INTO {}{} VALUES %s ON CONFLICT DO NOTHING".format(table_name, self.templates[table_name]), self.buff[table_name])
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
        self.buff["authors"].append((
            json_data["id"], json_data["author_id"], json_data["text"], json_data["possibly_sensitive"],
            json_data["lang"], json_data["source"], json_data["public_metrics"]["retweet_count"],
            json_data["public_metrics"]["reply_count"], json_data["public_metrics"]["like_count"],
            json_data["public_metrics"]["quote_count"], json_data["created_at"]
        ))

        self._do_upload("conversations")
        self.__upload_hashtags(json_data)
        self.__upload_context_annotations(json_data)
        self.__upload_annotations(json_data)
        self.__upload_links(json_data)
        self.__upload_references(json_data)

    def __upload_hashtags(self, json_data: dict):
        # annotations
        for item in json_data["entities"].get("hashtags", []):
            self.cursor.execute("""SELECT id FROM hashtags WHERE tag=%s""", [item["tag"]])

            tag_id = self.cursor.fetchone()[0]

            if tag_id is None:
                self.cursor.execute("""INSERT INTO hashtags(tag) VALUES (%s) RETURNING ID""", [item["tag"]])

            tag_id = self.cursor.fetchone()[0]

            self.buff["conversation_hashtags"].append((
                json_data["id"], tag_id
            ))

            self._do_upload("conversation_hashtags")

    def __upload_context_annotations(self, json_data: dict):
        # annotations
        for item in json_data.get("context_annotations", []):
            self.buff["context_annotations"].append((
                json_data["id"], item["domain"]["id"], item["entity"]["id"]
            ))
            self.buff["context_domains"].append((
                item["domain"]["id"], item["domain"]["name"], item["domain"]["description"]
            ))
            self.buff["context_entities"].append((
                item["entity"]["id"], item["entity"]["name"], item["entity"]["description"]
            ))
            self._do_upload("context_annotations")
            self._do_upload("context_domains")
            self._do_upload("context_entities")

    def __upload_annotations(self, json_data: dict):
        # annotations
        for item in json_data["entities"].get("annotations", []):
            self.buff["annotations"].append((
                json_data["id"], item["normalized_text"], item["type"], item["probability"]
            ))
            self._do_upload("annotations")

    def __upload_links(self, json_data: dict):
        # links
        for item in json_data["entities"].get("urls", []):
            self.buff["links"].append((
                json_data["id"], item["expanded_url"], item["title"], item["description"]
            ))
            self._do_upload("links")

    def __upload_references(self, json_data: dict):
        # conversation_references
        for item in json_data.get("referenced_tweets", []):
            self.buff["conversation_references"].append((
                json_data["id"], item["id"], item["type"]
            ))
        self._do_upload("conversation_references")

    def close(self):
        self._do_upload("conversations", force=True)
        self._do_upload("links", force=True)
        self._do_upload("annotations", force=True)
        self._do_upload("context_annotations", force=True)
        self._do_upload("context_domains", force=True)
        self._do_upload("context_entities", force=True)
        self._do_upload("conversation_hashtags", force=True)
        self._do_upload("conversation_references", force=True)
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
            uploader.upload(json_data)
            print(json_data)
            break
    uploader.close()


if __name__ == '__main__':
    main()
