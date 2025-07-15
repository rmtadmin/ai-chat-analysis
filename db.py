import mysql.connector
from mysql.connector import pooling
import json

db_config = {
    "host": "localhost",
    "user": "daily",
    "password": "zemhfKAM7&k\"G?P",
    "database": "aaa3dplus",
    "port": 3306,
}

pool = pooling.MySQLConnectionPool(pool_name="mypool", pool_size=5, **db_config)

def get_connection():
    return pool.get_connection()

def insert_json_result(session: dict):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO chat_sessions_json
        (id, source_file, created_at, host, model, result_json, success, error)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        session["id"],
        session["source_file"],
        session["created_at"],
        session["host"],
        session["model"],
        json.dumps(session["result"], ensure_ascii=False),
        session["success"],
        session.get("error"),
    ))
    conn.commit()
    cursor.close()
    conn.close()
