import os
import uuid
import hashlib
from datetime import datetime
from dotenv import load_dotenv
from server import talk_to_db, exists_in_table
from utilities import ( 
    get_content_from_url, 
    tokenize_and_embed_text, 
    embed_text_openAI, 
)
from r2 import upload_to_bucket, config_client

load_dotenv()

client = config_client(
    os.getenv('R2_ACCOUNT_ID'), 
    os.getenv('R2_ACCESS_KEY_ID'), 
    os.getenv('R2_SECRET_ACCESS_KEY')
)

def process_url(url, user_id):
    try:
        if exists_in_table('pages', {'url': f"{url}"}):
            return {
                'status': 300,
                'message': "This URL already exists in the DB"
            }
        
        if not exists_in_table('users', {'id': f"{user_id}"}):
            talk_to_db(
                "INSERT INTO suggestions (url, created_at) VALUES (%s, NOW())",
                (url,)
            )
            return {
                'status': 401,
                'message': f"User: {user_id} is unauthorized"
            }
        
        talk_to_db(
            "INSERT INTO queue (url, add_by, created_at) VALUES (%s, %s, NOW())",
            (url, user_id)
        )

        title, content = get_content_from_url(url)
        chunksList = tokenize_and_embed_text(content, 800, 0.5, 768)
        page_id = str(uuid.uuid4())

        talk_to_db(
            """
            INSERT INTO pages (id, created_at, title, url, embedding, added_by, date)
            VALUES (%s, NOW(), %s, %s, %s, %s, NOW(), %s);
            """,
            (page_id, title, url, embed_text_openAI(content, 1024), user_id)
        )

        for (content, embedding) in chunksList:
            key = f"{content}-{datetime.timestamp(datetime.now())}-{url}"
            key_encoded = key.encode('utf-8')
            key_hash = hashlib.md5(key_encoded).hexdigest()
            content = content.encode('utf-8')
            upload_to_bucket(client, content, key_hash, 'test-case')

            talk_to_db(
                "INSERT INTO chunks (page_id, content_id, embedding) VALUES (%s, %s, %s)",
                (page_id, key_hash, embedding)
            )

        talk_to_db(
            "DELETE FROM queue WHERE url=%s",
            (url,)
        )

        return {
            'status': 200,
            'message': "Successfully Embedded"
        }

    except Exception as e:
        return {
            'status': 500,
            'message': f"Internal server error: {e}"
        }