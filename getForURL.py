from server import get_data_from_db, talk_to_db, exists_in_table
from utilities import get_text_from_URL, LLM_Agent_for_title_desc, process_data, intersection_of_tuples, embed_text_openAI

def get_matches_for_url(url, user_id):
    try:
        if url.endswith('/'):
            url = url[:-1]

        heads, content = get_text_from_URL(url)
        title, desc = LLM_Agent_for_title_desc(heads, content)
        embeds = [embed_text_openAI(title, 768), embed_text_openAI(desc, 768)]

        results = []
        for embedding in embeds:
            retrieved_data = get_data_from_db(
                "SELECT page_id, embedding <=> %s::vector AS distance FROM chunks ORDER BY distance LIMIT %s",
                (embedding, 10)
            )
            results.append([point[0] for point in retrieved_data])
    
        urls = []
        for ID in intersection_of_tuples(results):
            urls.append(get_data_from_db(
                "SELECT url FROM pages WHERE id=%s",
                (ID,)
            ))

        if not exists_in_table('pages', {'url': f"{url}"}):
            talk_to_db(
                "INSERT INTO requests (created_at, content, type, user_id) VALUES (NOW(), %s, %s, %s)",
                (url, "url", user_id)
            )

        return {
            'status': 200,
            'urls': process_data(urls)
        }

    except Exception as e:
        print(f"Error processing URL request: {e}")
        return {
            'status': 500,
            'message': 'Internal server error'
        }