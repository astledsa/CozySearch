import os
import json
import requests
import tiktoken
from bs4 import BeautifulSoup

def convert_to_sorted_url_count_list(url_list):
    url_count = {}
    for item in url_list:
        url = item[0][0]  
        url_count[url] = url_count.get(url, 0) + 1
    
    sorted_url_count = [{"url": url, "count": count} 
                        for url, count in sorted(url_count.items(), key=lambda x: x[1], reverse=True)]
    
    return sorted_url_count

def intersection_of_tuples(tuples):
  
  if not tuples:
    raise Exception("Empty tuple provided")
  
  intersection = set(tuples[0])
  for t in tuples[1:]:
    intersection = intersection.intersection(set(t))

  return list(intersection)

def get_text_from_URL (url: str) :
    
    try:
        response = requests.get(url)
        response.raise_for_status()  
    except requests.RequestException as e:
        raise ConnectionRefusedError(f"Error fetching the URL: {e}")

    soup = BeautifulSoup(response.text, 'html.parser')
    titles = [tag.get_text(strip=True) for tag in soup.find_all(['h1'])]

    for script in soup(["script", "style"]):
        script.decompose()

    text = soup.get_text()
    for title in titles:
        text = text.replace(title, "") 

    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    body_text = '\n'.join(chunk for chunk in chunks if chunk)

    return titles, body_text

def embed_text_openAI (text: str, dim: int) :
    
    headers = {
       "Content-Type": "application/json",
       "Authorization": f"Bearer {os.environ['OPENAI_API_KEY']}"
    }
    
    data = {
       "input": text,
       "model": "text-embedding-3-small",
       "dimensions": dim
    }
    
    try:
        response = requests.post("https://api.openai.com/v1/embeddings", headers=headers, json=data)
        if response.status_code == 200:
            data = response.json()
            embedding = data['data'][0]['embedding']

            return embedding
        else:
            print(f"Error: API request failed with status code {response.status_code}")

    except requests.exceptions.RequestException as e:
        print(f"Error: An error occurred during the request: {e}")

def tokenize_and_embed_text (text : str, chunk_size: int, overlap: float, dim: int) :

    assert (overlap < 1.0)
    assert (overlap > 0.0)

    offset = int(chunk_size * overlap)
    end = chunk_size
    encoder = tiktoken.encoding_for_model("gpt-3.5-turbo")
    encodings = encoder.encode(text)
    
    start = 0
    lengthOfEncodings = len(encodings)
    TokenEmbeddingMatrix = list()
    while start < lengthOfEncodings :

        end = min(end, lengthOfEncodings)
        content = encoder.decode(encodings[start:end])
        TokenEmbeddingMatrix.append((content, embed_text_openAI(content, dim)))

        if end == lengthOfEncodings :
            break

        start += offset
        end += offset
    
    return TokenEmbeddingMatrix

def LLM_Agent_for_title_desc (heads, content) :

    url = "https://api.openai.com/v1/chat/completions"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}"
    }
    
    data = {
        "model": "gpt-4o-mini-2024-07-18",
        "messages": [
            {
                "role": "system",
                "content": """You are a summarizer. Give the title or titles and the content, give a short and
                              appropraite title of max 10 words, and a short and appropraite description of the 
                              content in no more than 60 words. The output should be in JSON format, with a 
                              title and a description attribute.
                            """
            },
            {
                "role": "user",
                "content": f'Title: {heads}, Content: {content}'
            }
        ]
    }
    try :
        response = requests.post(url, headers=headers, json=data)
    except: 
        raise Exception ("Error in getting the response")
    
    try: 
        res = response.json()['choices'][0]['message']['content']
        res = json.loads(res[8:-4]) 
    except: 
        raise Exception("Error in parsing")
    
    return res['title'], res['description']