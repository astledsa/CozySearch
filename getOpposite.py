from server import get_data_from_db, talk_to_db
from utilities import embed_text_openAI, process_data

def get_opposite(phrase, user_id):
    try:
        pageIDs = get_data_from_db(
            """SELECT page_id, 1 - (embedding <=> %s::vector) AS neg_distance
               FROM chunks
               ORDER BY neg_distance ASC
               LIMIT %s;""",
            (embed_text_openAI(phrase, 768), 10)
        )

        if pageIDs is None:
            raise Exception("Error in retrieval")
    
        urls = []

        for ID in pageIDs:
            urls.append(get_data_from_db(
                "SELECT (url, title) FROM pages WHERE id=%s",
                (ID[0],)
            ))

        talk_to_db(
            "INSERT INTO requests (created_at, content, type, user_id) VALUES (NOW(), %s, %s, %s)",
            (phrase, "phrase", user_id)
        )

        return {
            'status': 200,
            'urls': process_data(urls)
        }

    except Exception as e:
        print(f"Error processing phrase: {e}")
        return {
            'status': 500,
            'message': 'Internal Server Error'
        }