from server import get_data_from_db, talk_to_db
from utilities import embed_text_openAI, process_data, intersection_of_tuples

def get_matches_for_words(words, user_id):
    try:
        results = []
        embedded_words = [embed_text_openAI(word, 768) for word in words]

        for embedding in embedded_words:
            retrieved_data = get_data_from_db(
                "SELECT page_id, embedding <=> %s::vector AS distance FROM chunks ORDER BY distance LIMIT %s",
                (embedding, 10)
            )
        
            results.append([point[0] for point in retrieved_data])
    
        urls = []
        for IDs in intersection_of_tuples(results):
            urls.append(get_data_from_db(
                "SELECT (url, title) FROM pages WHERE id=%s",
                (IDs,)
            ))
    
        talk_to_db(
            "INSERT INTO requests (created_at, content, type, user_id) VALUES (NOW(), %s, %s, %s)",
            (", ".join(words), "words", user_id)
        )

        return {
            'status': 200,
            'urls': process_data(urls)
        }

    except Exception as e:
        print(f"Error processing words: {e}")
        return {
            'status': 500,
            'message': 'Internal Server Error'
        }