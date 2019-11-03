"""
Module related to the database management
"""
import sqlite3
import json
from datetime import datetime

timeframe = '2015-05'
sql_transaction = []

CONNECTION = sqlite3.connect('{}.db'.format(timeframe))
CURSOR = CONNECTION.cursor()


def create_table():
    """
    Creates a table if it does not already exist
    """
    CURSOR.execute("CREATE TABLE IF NOT EXISTS parent_reply(parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT, subreddit TEXT, unix INT, score INT)")

def format_data(data):
    """
    Does the clean ups required
    :return : formatted data
    """
    data = data.replace("\n", " newlinechar ").replace("\r", " returnchar ").replace('"', "'")
    return data

def find_parent(pid):
    """
    find if the parent comment
    :return : the comment if parent, otherwise false
    """
    try:
        sql = "SELECT comment from parent_reply where comment_id = '{}' LIMIT 1".format(pid)
        CURSOR.execute(sql)
        result = CURSOR.fetchone()
        if result is not None:
            return result[0]
        return False
    except Exception as e:
        print("find_parent", e)
        return False

if __name__ == '__main__':
    create_table()
    row_counter = 0
    paired_rows = 0
    with open('/home/firaaz/projects/reddit_chat_data/RC_2015-01', buffering=1000) as f:
        for row in f:
            row_counter += 1
            row = json.loads(row)
            parent_id = row['parent_id']
            body = row['body']
            score = row['score']
            subreddit = row['subreddit']

            parent_data = find_parent(parent_id)
