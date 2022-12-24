import os
import time
import pandas as pd
from elasticsearch import Elasticsearch
from elasticsearch.helpers import parallel_bulk, bulk
import psycopg2
from psycopg2.extras import RealDictCursor


QUERY_ALL = """
    SELECT con.*,
        au.name as author_name, au.username as author_username, au.description as author_description, au.followers_count, au.following_count, au.tweet_count, au.listed_count,
        an.type as ann_type, an.value as ann_value, an.probability as ann_probability,
        ctxe.name as entity_name, ctxe.description as entity_description, ctxd.name as domain_name, ctxd.description as domain_description,
        links.url, links.title as link_title, links.description as link_description, ha.tag,
        conrd.parent_id as ref_parent_id, conrd.type as ref_type, conrd.id as ref_author_id, conrd.name as ref_author_name, conrd.username as ref_author_username,
            conrd.content as ref_content, conrd.array_agg as ref_tags
    FROM conversations con LEFT JOIN
        authors au ON con.author_id=au.id LEFT JOIN
        context_annotations ctxa ON con.id=ctxa.conversation_id LEFT JOIN
        context_domains ctxd ON ctxd.id=ctxa.context_domain_id LEFT JOIN
        context_entities ctxe ON ctxe.id=ctxa.context_entity_id LEFT JOIN
        annotations an ON con.id=an.conversation_id LEFT JOIN
        conversation_hashtags conh ON con.id=conh.conversation_id LEFT JOIN
        conversation_references_denormalized conrd ON con.id=conrd.conversation_id LEFT JOIN
        hashtags ha ON ha.id=conh.hashtag_id LEFT JOIN
        links ON con.id=links.conversation_id
    WHERE con.id>%s
    ORDER BY con.id ASC
    LIMIT 150000
    """

QUERY_FIRST = """
    SELECT con.*,
        au.name as author_name, au.username as author_username, au.description as author_description, au.followers_count, au.following_count, au.tweet_count, au.listed_count,
        an.type as ann_type, an.value as ann_value, an.probability as ann_probability,
        ctxe.name as entity_name, ctxe.description as entity_description, ctxd.name as domain_name, ctxd.description as domain_description,
        links.url, links.title as link_title, links.description as link_description, ha.tag,
        conrd.parent_id as ref_parent_id, conrd.type as ref_type, conrd.id as ref_author_id, conrd.name as ref_author_name, conrd.username as ref_author_username,
            conrd.content as ref_content, conrd.array_agg as ref_tags
    FROM conversations con LEFT JOIN
        authors au ON con.author_id=au.id LEFT JOIN
        context_annotations ctxa ON con.id=ctxa.conversation_id LEFT JOIN
        context_domains ctxd ON ctxd.id=ctxa.context_domain_id LEFT JOIN
        context_entities ctxe ON ctxe.id=ctxa.context_entity_id LEFT JOIN
        annotations an ON con.id=an.conversation_id LEFT JOIN
        conversation_hashtags conh ON con.id=conh.conversation_id LEFT JOIN
        conversation_references_denormalized conrd ON con.id=conrd.conversation_id LEFT JOIN
        hashtags ha ON ha.id=conh.hashtag_id LEFT JOIN
        links ON con.id=links.conversation_id
    ORDER BY con.id ASC
    LIMIT 150000
    """

QUERY_REMAINDER = """
    SELECT con.*,
        au.name as author_name, au.username as author_username, au.description as author_description, au.followers_count, au.following_count, au.tweet_count, au.listed_count,
        an.type as ann_type, an.value as ann_value, an.probability as ann_probability,
        ctxe.name as entity_name, ctxe.description as entity_description, ctxd.name as domain_name, ctxd.description as domain_description,
        links.url, links.title as link_title, links.description as link_description, ha.tag,
        conrd.parent_id as ref_parent_id, conrd.type as ref_type, conrd.id as ref_author_id, conrd.name as ref_author_name, conrd.username as ref_author_username,
            conrd.content as ref_content, conrd.array_agg as ref_tags
    FROM conversations con LEFT JOIN
        authors au ON con.author_id=au.id LEFT JOIN
        context_annotations ctxa ON con.id=ctxa.conversation_id LEFT JOIN
        context_domains ctxd ON ctxd.id=ctxa.context_domain_id LEFT JOIN
        context_entities ctxe ON ctxe.id=ctxa.context_entity_id LEFT JOIN
        annotations an ON con.id=an.conversation_id LEFT JOIN
        conversation_hashtags conh ON con.id=conh.conversation_id LEFT JOIN
        conversation_references_denormalized conrd ON con.id=conrd.conversation_id LEFT JOIN
        hashtags ha ON ha.id=conh.hashtag_id LEFT JOIN
        links ON con.id=links.conversation_id
    WHERE con.id=%s
    """


def process_batch(data: list):
    denormalized = []

    last_id = ""  # initialize
    for row in data:

        # new conversation
        if last_id != row["id"]:
            item = {
                "id": row["id"],
                "author": {
                    "id": row["author_id"],
                    "name": row["author_name"],
                    "username": row["author_username"],
                    "description": row["author_description"],
                    "followers_count": row["followers_count"],
                    "following_count": row["following_count"],
                    "tweet_count": row["tweet_count"],
                    "listed_count": row["listed_count"],
                },
                "content": row["content"],
                "possibly_sensitive": row["possibly_sensitive"],
                "language": row["language"],
                "source": row["source"],
                "retweet_count": row["retweet_count"],
                "reply_count": row["reply_count"],
                "like_count": row["like_count"],
                "quote_count": row["quote_count"],
                "created_at": row["created_at"],
                "entities": [],
                "domains": [],
                "annotations": [],
                "references": [],
                "hashtags": [],
                "links": [],
            }

        if row["entity_name"] is not None and not any(
                d.get('name', "") == row["entity_name"] for d in item["entities"]):
            item["entities"].append({
                "name": row["entity_name"],
                "description": row["entity_description"],
            })

        if row["domain_name"] is not None and not any(
                d.get('name', "") == row["domain_name"] for d in item["domains"]):
            item["domains"].append({
                "name": row["domain_name"],
                "description": row["domain_description"],
            })

        # annotations
        if row["ann_value"] is not None and not any(d.get('value', "") == row["ann_value"] for d in item["annotations"]):
            item["annotations"].append({
                "value": row["ann_value"],
                "type": row["ann_type"],
                "probability": row["ann_probability"],
            })

        # references
        if row["ref_parent_id"] is not None and not any(
                d.get('id', "") == row["ref_parent_id"] for d in item["references"]):
            item["references"].append({
                "id": row["ref_parent_id"],
                "type": row["ref_type"],
                "author": {
                    "id": row["ref_author_id"],
                    "name": row["ref_author_name"],
                    "username": row["ref_author_username"],
                },
                "content": row["ref_content"],
                "tags": row["ref_tags"]
            })

        # hashtags
        if row["tag"] is not None and not row["tag"] in item["hashtags"]:
            item["hashtags"].append(row["tag"])

        # links
        if row["url"] is not None and not any(d.get('url', "") == row["url"] for d in item["links"]):
            item["links"].append({
                "url": row["url"],
                "title": row["link_title"],
                "description": row["link_description"]
            })

        # new conversation
        if last_id != row["id"]:
            denormalized.append(item)

        last_id = item["id"]
    return denormalized


def main():
    conn = psycopg2.connect(
        host=os.environ["DB_HOST"],
        port=5432,
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
        database=os.environ["DB_DATABASE"],
    )
    cursor = conn.cursor(cursor_factory=RealDictCursor)
    # 626 mil :)
    es = Elasticsearch("https://localhost:9200", verify_certs=False, http_auth=("elastic", "elastic"))

    tbt = time.time()
    last_id = 1507689002165424134
    while last_id is not None:
        bt = time.time()

        if last_id == 0:
            cursor.execute(QUERY_FIRST)
        else:
            cursor.execute(QUERY_ALL, [int(last_id)])
        res = cursor.fetchall()

        # fetch remaining rows for the last ID since it wont be in the next iteration
        try:
            last_id = res[149999]["id"]
        except Exception:
            last_id = res[len(res) - 1]["id"]

        cursor.execute(QUERY_REMAINDER, [int(last_id)])
        res += cursor.fetchall()

        denormalized = process_batch(res)

        res = bulk(es, denormalized, index='tweets', stats_only=True)

        et = time.time()

        print("processed_ID:" + str(last_id) + ",elaspsed: " + str(et-bt))

        try:
            res[149999]["id"]
        except Exception:
            last_id = None

    cursor.close()
    tet = time.time()
    print("total time elaspesd:" + str(tet-tbt))
    conn.close()


if __name__ == '__main__':
    main()
