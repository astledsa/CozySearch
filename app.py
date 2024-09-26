import os
import uuid
import hashlib
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from r2 import config_client, upload_to_bucket
from server import talk_to_db, get_data_from_db, exists_in_table
from utilities import convert_to_sorted_url_count_list, intersection_of_tuples
from utilities import get_text_from_URL, tokenize_and_embed_text, LLM_Agent_for_title_desc, embed_text_openAI

load_dotenv()
app = Flask(__name__)

client = config_client(
    os.getenv('R2_ACCOUNT_ID'), 
    os.getenv('R2_ACCESS_KEY_ID'), 
    os.getenv('R2_SECRET_ACCESS_KEY')
)

@app.route('/api/post/url', methods=['POST'])
def sendURL () :

    try:
        user_id = request.json.get('user_id')
        url = request.json.get('url')

        if user_id is None or url is None:
            raise Exception ("No URL or user ID provided")
        if url.endswith('/'):
            url = url[:-1]

    except Exception as e :

        return jsonify({
            'status': 400,
            'message': f"Error in parsing query parameters, {e}."
        })
    
    print(f"Uploading: {url}")
    
    if exists_in_table( 'pages', {'url': f"{url}"}) :
        return jsonify({
            'status': 300,
            'message': "This URL already exists in the DB"
        })
    
    if not exists_in_table ( 'users', {'id': f"{user_id}"}) :

        talk_to_db (
            "INSERT INTO suggestions (url, created_at) VALUES (%s, NOW())",
            (url,)
        )

        return jsonify({
            'status': 401,
            'message': f"User: {user_id} is unauthorized"
        })
    
    else:
        talk_to_db (
            "INSERT INTO queue (url, add_by, created_at) VALUES (%s, %s, NOW())",
            (url, user_id)
        )

        try :

            heads, content = get_text_from_URL(url)
            chunksList = tokenize_and_embed_text(content, 800, 0.5, 768)
            title, description = LLM_Agent_for_title_desc(heads, content)
            page_id = str(uuid.uuid4())

            talk_to_db(
                """
                INSERT INTO pages (id, created_at, title, url, embedding, added_by, date, description)
                VALUES (%s, NOW(), %s, %s, %s, %s, NOW(), %s);
                """,
                (page_id, title, url, embed_text_openAI(content, 1024), user_id, description)
            )

            for (content, embedding) in chunksList :
                key = f"{content}-{datetime.timestamp(datetime.now())}-{url}"
                key_encoded = key.encode('utf-8')
                key_hash = hashlib.md5(key_encoded).hexdigest()
                content = content.encode('utf-8')
                upload_to_bucket(client, content, key_hash, 'test-case')

                talk_to_db (
                    "INSERT INTO chunks (page_id, content_id, embedding) VALUES (%s, %s, %s)",
                    (page_id, key_hash, embedding)
                )

        except Exception as e: 
            return jsonify({
                'status': 500,
                'message' : f"Internal server error: {e}"
            })

        talk_to_db (
            "DELETE FROM queue WHERE url=%s",
            (url,)
        )

        print("Done!")

        return jsonify({
            'status': 200,
            'message': "Successfully Embedded"
        })

@app.route('/api/post/bulk', methods=['POST'])
def sendMultipleURLs():
    try:
        user_id = request.json.get('user_id')
        urls = request.json.get('urls')
        if not isinstance(urls, list):
            raise ValueError("URLs must be provided as a list")
        
    except Exception as e:
        return jsonify({
            'status': 400,
            'message': f"Error in parsing query parameters: {e}."
        })

    if not exists_in_table( 'users', {'id': f"{user_id}"}):
        for url in urls:
            talk_to_db(
                
                "INSERT INTO suggestions (url, created_at) VALUES (%s, NOW())",
                (url,)
            )
        return jsonify({
            'status': 401,
            'message': f"User: {user_id} is unauthorized"
        })

    results = []
    for url in urls:
        if exists_in_table( 'pages', {'url': f"{url}"}):
            results.append({
                'url': url,
                'status': 300,
                'message': "This URL already exists in the DB"
            })
            continue

        talk_to_db(
            
            "INSERT INTO queue (url, add_by, created_at) VALUES (%s, %s, NOW())",
            (url, user_id)
        )

        try:
            heads, content = get_text_from_URL(url)
            chunksList = tokenize_and_embed_text(content, 800, 0.5, 768)
            title, description = LLM_Agent_for_title_desc(heads, content)
            page_id = str(uuid.uuid4())

            talk_to_db(
                
                """
                INSERT INTO pages (id, created_at, title, url, embedding, added_by, date, description)
                VALUES (%s, NOW(), %s, %s, %s, %s, NOW(), %s);
                """,
                (page_id, title, url, embed_text_openAI(content, 1024), user_id, description)
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

            results.append({
                'url': url,
                'status': 200,
                'message': "Successfully Embedded"
            })

        except Exception as e:
            results.append({
                'url': url,
                'status': 500,
                'message': f"Internal server error: {e}"
            })

    return jsonify({
        'status': 200,
        'message': 'URLs uploaded',
        'results': results
    })

@app.route('/api/get/phrase', methods=['GET'])
def getThroughPhrase () :

    try :
        phrase = request.args.get('sentence')
        user_id = request.args.get('user_id')

        if phrase is None or user_id is None :
            raise Exception("Phrase of user_id must be provided")
        
    except:
        return jsonify({
            'status': 400,
            'Message': 'Error in parsing the query'
        })
    
    print(f"Attempting to retrieve for phrase: {phrase}")
    
    try:
        pageIDs = get_data_from_db (
            
            "SELECT page_id, embedding <=> %s::vector AS distance FROM chunks ORDER BY distance LIMIT %s",
            (embed_text_openAI(phrase, 768), 10)
        )

        if pageIDs is None :
            raise Exception ("Error in retrieval")
    
        urls = list()

        for ID in pageIDs :
            urls.append(get_data_from_db (
                
                "SELECT (url, title) FROM pages WHERE id=%s",
                (ID[0],)
            ))

    except :
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error'
        })
    
    talk_to_db (
        
        "INSERT INTO requests (created_at, content, type, user_id) VALUES (NOW(), %s, %s, %s)",
        (phrase, "phrase", user_id)
    )

    print("Done")
    
    return jsonify ({
        'status': 200,
        'urls': convert_to_sorted_url_count_list (urls)
    })
    
@app.route('/api/get/words', methods=['GET'])
def getThroughWords ():

    try:
        words = request.json.get('words')
        user_id = request.json.get('user_id')

        if words is None or words == [] or user_id is None:
            raise Exception ("Invalid query params")
        
    except Exception as e:
        return jsonify({
            'status': 400,
            'Message': f'Error in parsing the query, {e}'
        })
    
    print("Retrievig based on words: ", words)

    try:
        results = list()
        embeddedWords = [embed_text_openAI(word, 768) for word in words]

        for embedings in embeddedWords :
            retreivedData = get_data_from_db(
                
                "SELECT page_id, embedding <=> %s::vector AS distance FROM chunks ORDER BY distance LIMIT %s",
                (embedings, 10)
            )
        
            results.append([point[0] for point in retreivedData])
    
        urls = list()
        for IDs in intersection_of_tuples(results) :
            urls.append(get_data_from_db (
                
                "SELECT url FROM pages WHERE id=%s",
                (IDs,)
            ))
    
    except:
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error'
        })
    
    talk_to_db (
        
        "INSERT INTO requests (created_at, content, type, user_id) VALUES (NOW(), %s, %s, %s)",
        (", ".join(words), "words", user_id)
    )

    print("Done")

    return jsonify({
        'status': 200,
        'urls' : convert_to_sorted_url_count_list(urls)
    })
    
@app.route('/api/get/document', methods=['GET'])
def getThroughDoc ():

    try:
        doc = request.json.get('document')
        user_id = request.json.get('user_id')

        if doc is None or user_id is None :
            raise Exception ("Invalid query params")
    
    except Exception as e:
        return jsonify({
            'status': 400,
            'message': f"Error parsing the request body, {e}"
        })
    
    print("Retrieving based on document")

    try:
        urls = get_data_from_db (
            
            "SELECT url, embedding <=> %s::vector AS distance FROM pages ORDER BY distance LIMIT %s",
            (embed_text_openAI(doc, 1024), 10)
        )

    except Exception as e:
        print(e)
        return jsonify({
            'status': 500,
            'message': "Internal server error"
        })
    
    talk_to_db (
        
        "INSERT INTO requests (created_at, content, type, user_id) VALUES (NOW(), %s, %s, %s)",
        (doc, "document", user_id)
    )
    
    print("Done")

    return jsonify({
        'status': 200,
        'urls': [url[0] for url in urls]
    })

@app.route('/api/get/url', methods=['GET'])
def getThroughURL ():

    try:

        url = request.json.get('url')
        user_id = request.json.get('user_id')

        if url is None or user_id is None :
            raise Exception("URL and user_id must be provided")
        if url.endswith('/'):
            url = url[:-1]

    except Exception as e :

        return jsonify({
            'status': 400,
            'message': f"Error parsing message, {e}"
        })
    
    print("Retrieving based on url")
    
    try :
        heads, content = get_text_from_URL(url)
        title, desc = LLM_Agent_for_title_desc(heads, content)
        embeds = [embed_text_openAI(title, 768), embed_text_openAI(desc, 768)]

        results = list()
        for embedings in embeds :
            retreivedData = get_data_from_db(
                
                "SELECT page_id, embedding <=> %s::vector AS distance FROM chunks ORDER BY distance LIMIT %s",
                (embedings, 10)
            )
        
            results.append([point[0] for point in retreivedData])
    
        urls = list()
        for IDs in intersection_of_tuples(results) :
            urls.append(get_data_from_db (
                
                "SELECT url FROM pages WHERE id=%s",
                (IDs,)
            ))
    except:
        return jsonify({
            'status': 500,
            'message': 'Internal server error'
        })
    

    if not exists_in_table ( 'pages', {'url': f"{url}"}):
        talk_to_db (
            
            "INSERT INTO requests (created_at, content, type, user_id) VALUES (NOW(), %s, %s, %s)",
            (url, "url", user_id)
        )
    
    print("Done")

    return jsonify({
        'status': 200,
        'urls': convert_to_sorted_url_count_list(urls) 
    })

@app.route('/api/get/opposite', methods=['GET'])
def getOpposite ():
    try :
        phrase = request.json.get('sentence')
        user_id = request.json.get('user_id')

        if phrase is None or user_id is None :
            raise Exception("Phrase of user_id must be provided")
        
    except:
        return jsonify({
            'status': 400,
            'Message': 'Error in parsing the query'
        })
    
    print(f"Attempting to retrieve opposite of phrase: {phrase}")
    
    try:
        pageIDs = get_data_from_db (
            
            """SELECT page_id, 1 - (embedding <=> %s::vector) AS neg_distance
               FROM chunks
               ORDER BY neg_distance ASC
               LIMIT %s;""",
            (embed_text_openAI(phrase, 768), 10)
        )

        if pageIDs is None :
            raise Exception ("Error in retrieval")
    
        urls = list()

        for ID in pageIDs :
            urls.append(get_data_from_db (
                
                "SELECT url FROM pages WHERE id=%s",
                (ID[0],)
            ))

    except :
        return jsonify({
            'status': 500,
            'message': 'Internal Server Error'
        })
    
    talk_to_db (
        
        "INSERT INTO requests (created_at, content, type, user_id) VALUES (NOW(), %s, %s, %s)",
        (phrase, "phrase", user_id)
    )

    print("Done")
    
    return jsonify ({
        'status': 200,
        'urls': convert_to_sorted_url_count_list (urls)
    })

if __name__ == '__main__':
    app.run(debug=True)


