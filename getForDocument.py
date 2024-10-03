from server import get_data_from_db, talk_to_db
from utilities import embed_text_openAI

def get_matches_for_doc(doc, user_id):
    try:
        urls = get_data_from_db(
            "SELECT url, embedding <=> %s::vector AS distance FROM pages ORDER BY distance LIMIT %s",
            (embed_text_openAI(doc, 1024), 10)
        )

        talk_to_db(
            "INSERT INTO requests (created_at, content, type, user_id) VALUES (NOW(), %s, %s, %s)",
            (doc, "document", user_id)
        )

        return {
            'status': 200,
            'urls': [url[0] for url in urls]
        }

    except Exception as e:
        print(f"Error processing document: {e}")
        return {
            'status': 500,
            'message': 'Internal Server Error'
        }