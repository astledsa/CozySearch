import threading
from dotenv import load_dotenv
from flask import Flask, request, jsonify
from getForDocument import get_matches_for_doc
from getForPhrase import get_matches_for_phrase
from getForURL import get_matches_for_url
from getForWords import get_matches_for_words
from getOpposite import get_opposite
from processURL import process_url
from server import exists_in_table, get_data_from_db


load_dotenv()
app = Flask(__name__)

@app.route('/get/phrase', methods=['GET'])
def getWithWords():
    try: 
        phrase = request.args['sentence']
        user_id = request.args['user_id']

    except Exception as e:
        return jsonify({
            'status': 400,
            'message': f"Missing params:, {e}"
        }), 400
    
    result = get_matches_for_phrase(phrase, user_id)
    return jsonify(result), result['status']

@app.route('/get/url', methods=['GET'])
def getThroughURL():
    try:
        url = request.json.get('url')
        user_id = request.json.get('user_id')

        if url is None or user_id is None:
            raise Exception("URL and user_id must be provided")

    except Exception as e:
        return jsonify({
            'status': 400,
            'message': f"Error parsing message, {e}"
        }), 400
    
    print("Retrieving based on url...")
    
    result = get_matches_for_url(url, user_id)
    return jsonify(result), result['status']

@app.route('/get/opposite', methods=['GET'])
def getOpposite():
    try:
        phrase = request.json.get('sentence')
        user_id = request.json.get('user_id')

        if phrase is None or user_id is None:
            raise Exception("Phrase or user_id must be provided")
        
    except Exception as e:
        print(e)
        return jsonify({
            'status': 500,
            'message': f'Error in parsing the query: {str(e)}'
        }), 500
    
    result = get_opposite(phrase, user_id)
    return jsonify(result), result['status']

@app.route('/get/words', methods=['GET'])
def getThroughWords():
    words = request.json['words']
    user_id = request.json['user_id']
    
    if not words:
        return jsonify({
            'status': 400,
            'message': 'Words list cannot be empty'
        }), 400

    result = get_matches_for_words(words, user_id)
    return jsonify(result), result['status']

@app.route('/get/document', methods=['GET'])
def getThroughDoc():
    doc = request.json['document']
    user_id = request.json['user_id']
    
    result = get_matches_for_doc(doc, user_id)
    return jsonify(result), result['status']

@app.route('/post/url', methods=['POST'])
def sendURL():
    try:
        user_id = request.json.get('user_id')
        url = request.json.get('url')

        if user_id is None or url is None:
            raise Exception("No URL or user ID provided")
        if url.endswith('/'):
            url = url[:-1]

    except Exception as e:
        print(e)
        return jsonify({
            'status': 400,
            'message': f"Error in parsing query parameters, {e}."
        }), 400

    # Start processing in a separate thread
    thread = threading.Thread(target=process_url_async, args=(url, user_id))
    thread.start()

    # Immediately return a 200 status
    return jsonify({
        'status': 200,
        'message': "URL accepted for processing"
    }), 200

@app.route('/post/bulk', methods=['POST'])
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
        }), 400

    thread = threading.Thread(target=process_urls_async, args=(urls, user_id))
    thread.start()

    return jsonify({
        'status': 200,
        'message': f"{len(urls)} URLs accepted for processing"
    }), 200

@app.route('/get/stats', methods=['GET'])
def getStats () :
    # try:
    #     user_id = request.json.get('user_id')

    # except Exception as e:
    #     return jsonify({
    #         'status': 400,
    #         'message': f"Error in parsing query parameters: {e}."
    #     }), 400
    
    # if not exists_in_table("users", {"id": f"{user_id}"}) :
    #     return jsonify({
    #         'status': 401,
    #         'message': f"User {user_id} is not authorized"
    #     })
    
    try:
        no_of_links = get_data_from_db("SELECT COUNT(*) FROM pages")
        no_of_requests = get_data_from_db("SELECT COUNT(*) FROM requests")
        no_in_queue = get_data_from_db("SELECT COUNT(*) FROM queue")
    except:
        return jsonify({
            'status': 500,
            'message': "Internal Server Error (in retrieving values from Db)"
        })
    
    return jsonify({
        'status': 200,
        'message': {
            'links_added': no_of_links,
            'searches': no_of_requests,
            'links_in_queue': no_in_queue
        }
    })
   
    
def process_urls_async(urls, user_id):
    results = []
    for url in urls:
        result = process_url(url, user_id)
        results.append({
            'url': url,
            'status': result['status'],
            'message': result['message']
        })
    
    print(f"URLs processing results: {results}")
 
def process_url_async(url, user_id):
    result = process_url(url, user_id)
    
    print(f"URL processing result: {result}")


if __name__ == '__main__':
    app.run(debug=True)


