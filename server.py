import os
import psycopg2
from pgvector.psycopg2 import register_vector

def talk_to_db(
        query : str,
        values : tuple
    ):

    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))

        cursor = conn.cursor()

        register_vector(conn)
        cursor.execute(query, values)

        conn.commit()
        cursor.close()

    except Exception as e:
        print(f"Error performing the actionr: {e}")


def get_data_from_db(query, values=None):
    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))

        with conn.cursor() as cursor:
            cursor.execute(query, values)
            results = cursor.fetchall()
        return results
    except Exception as e:
        print(f"Error in retrieval: {e}")
        return None  

def exists_in_table (table_name, conditions):

    try:
        conn = psycopg2.connect(os.getenv('DATABASE_URL'))
        
        cursor = conn.cursor()
        where_clause = " AND ".join([f"{key} = %s" for key in conditions.keys()])
        query = f"SELECT EXISTS(SELECT 1 FROM {table_name} WHERE {where_clause});"
        cursor.execute(query, tuple(conditions.values()))
        result = cursor.fetchone()[0]

        return result

    except Exception as error:

        return False