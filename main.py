import os
from datetime import datetime, timezone
import gzip
from orjson import orjson
import psycopg2
from psycopg2.extras import execute_values


class Uploader:

    templates = None

    def __init__(self):
        self.conn = psycopg2.connect(
            host=os.environ["DB_HOST"],
            port=5432,
            user=os.environ["DB_USER"],
            password=os.environ["DB_PASSWORD"],
            database=os.environ["DB_DATABASE"],
        )
        self.cursor = self.conn.cursor()
        self.bt = datetime.now(timezone.utc)
        self.et = None
        self.output = open("output.csv", "a+")
        self.last_time = datetime.now(timezone.utc)
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
        self.output.close()
        self.et = datetime.now(timezone.utc)
        print("Total time elapsed: {}".format(self.et - self.bt))

    def _do_upload(self, table_name, force=False, log=False):
        if len(self.buff[table_name]) % 50000 == 0 or force:
            if log:
                utc_now = datetime.now(timezone.utc)
                block_min, block_s = divmod((utc_now - self.last_time).seconds, 60)
                total_min, total_s = divmod((utc_now - self.bt).seconds, 60)
                print("{}Z,{:02d}:{:02d},{:02d}:{:02d}".format(
                    utc_now.isoformat(timespec="seconds"),
                    total_min, total_s,
                    block_min, block_s
                    ), file=self.output)
                self.last_time = utc_now
            execute_values(self.cursor, "INSERT INTO {}{} VALUES %s ON CONFLICT DO NOTHING".format(table_name, self.templates[table_name]), self.buff[table_name], page_size=10000)
            self.conn.commit()
            self.buff[table_name] = []


class AuthorsUploader(Uploader):
    templates: dict[str, str] = {
        "authors": "(id, name, username, description, followers_count, following_count, tweet_count, listed_count)",
    }

    def upload(self, json_data: dict):
        self.buff["authors"].append((
            json_data["id"], json_data["name"], json_data["username"], json_data["description"],
            json_data["public_metrics"]["followers_count"], json_data["public_metrics"]["following_count"],
            json_data["public_metrics"]["tweet_count"], json_data["public_metrics"]["listed_count"]
        ))
        self._do_upload("authors", log=True)

    def close(self):
        if len(self.buff["authors"]) > 0:
            self._do_upload("authors", force=True, log=True)
        super().close()


class ConversationsUploader(Uploader):
    templates: dict[str, str] = {
        "authors": "(id)",
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

    def upload(self, json_data: dict):
        public_metrics = json_data.get("public_metrics", {})
        self.buff["conversations"].append((
            json_data["id"], json_data["author_id"], json_data["text"], json_data["possibly_sensitive"],
            json_data["lang"], json_data["source"], public_metrics.get("retweet_count"),
            public_metrics.get("reply_count"),  public_metrics.get("like_count"),
            public_metrics.get("quote_count"), json_data["created_at"]
        ))

        self._do_upload("conversations", log=True)
        self.__upload_authors(json_data)
        self.__upload_hashtags(json_data)
        self.__upload_context_annotations(json_data)
        self.__upload_annotations(json_data)
        self.__upload_links(json_data)
        self.__upload_references(json_data)

    def __upload_hashtags(self, json_data: dict):
        # annotations
        try:
            for item in json_data["entities"].get("hashtags", []):
                self.cursor.execute("""SELECT id FROM hashtags WHERE tag=%s""", [item["tag"]])

                tag_id = self.cursor.fetchone()

                if tag_id is None:
                    self.cursor.execute("""INSERT INTO hashtags(tag) VALUES (%s) RETURNING ID""", [item["tag"]])
                    tag_id = self.cursor.fetchone()[0]
                else:
                    tag_id = tag_id[0]

                self.buff["conversation_hashtags"].append((
                    json_data["id"], tag_id
                ))

                self._do_upload("conversation_hashtags")
        except KeyError:
            pass

    def __upload_context_annotations(self, json_data: dict):
        # annotations
        for item in json_data.get("context_annotations", []):
            self.buff["context_annotations"].append((
                json_data["id"], item["domain"]["id"], item["entity"]["id"]
            ))
            self.buff["context_domains"].append((
                item["domain"]["id"], item["domain"]["name"], item["domain"].get("description", None)
            ))
            self.buff["context_entities"].append((
                item["entity"]["id"], item["entity"]["name"], item["entity"].get("description", None)
            ))
            self._do_upload("context_annotations")
            self._do_upload("context_domains")
            self._do_upload("context_entities")

    def __upload_authors(self, json_data):
        self.buff["authors"].append((
            json_data["author_id"],
        ))
        self._do_upload("authors")

    def __upload_annotations(self, json_data: dict):
        # annotations
        try:
            for item in json_data["entities"].get("annotations", []):
                self.buff["annotations"].append((
                    json_data["id"], item["normalized_text"], item["type"], item["probability"]
                ))
                self._do_upload("annotations")
        except KeyError:
            pass

    def __upload_links(self, json_data: dict):
        # links
        try:
            for item in json_data["entities"].get("urls", []):
                if len(item["expanded_url"]) > 2048:
                    continue
                self.buff["links"].append((
                    json_data["id"], item["expanded_url"], item.get("title", None), item.get("description")
                ))
                self._do_upload("links")
        except KeyError:
            pass

    def __upload_references(self, json_data: dict):
        # conversation_references
        for item in json_data.get("referenced_tweets", []):
            self.buff["conversation_references"].append((
                json_data["id"], item["id"], item["type"]
            ))
        self._do_upload("conversation_references")

    def close(self):
        self._do_upload("conversations", force=True, log=True)
        self._do_upload("links", force=True)
        self._do_upload("annotations", force=True)
        self._do_upload("context_annotations", force=True)
        self._do_upload("context_domains", force=True)
        self._do_upload("context_entities", force=True)
        self._do_upload("conversation_hashtags", force=True)
        self._do_upload("conversation_references", force=True)
        super().close()


def main():

    uploader = AuthorsUploader()
    with gzip.open("D:\\Downloads\\authors.jsonl.gz", 'rt') as f:
        for line in f:
            json_data = orjson.loads(line.replace("\\u0000", ""))  # need to replace this because postgres wont accept \x00 cahracter
            uploader.upload(json_data)
    uploader.close()

    uploader = ConversationsUploader()
    with gzip.open("D:\\Downloads\\conversations.jsonl.gz", 'rt') as f:
        for line in f:
            json_data = orjson.loads(line.replace("\\u0000", ""))  # need to replace this because postgres wont accept \x00 cahracter
            uploader.upload(json_data)
    uploader.close()


if __name__ == '__main__':
    main()
