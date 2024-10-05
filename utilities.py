import re
import os
import requests
import tiktoken
from bs4 import BeautifulSoup
from collections import Counter
from urllib.parse import urlparse

def process_data(input_data):
    flattened_data = [item[0] for sublist in input_data for item in sublist]

    processed_data = []
    for item in flattened_data:
        url, title = item.strip('()').split(',', 1)
        url = url.strip('"')
        title = title.strip().strip('"')
        processed_data.append((url, title))
    
    counter = Counter(processed_data)

    result = [
        {
            "url": url,
            "title": title,
            "count": count
        }
        for (url, title), count in counter.items()
    ]
    
    return result

def intersection_of_tuples(tuples):
  
  if not tuples:
    raise Exception("Empty tuple provided")
  
  intersection = set(tuples[0])
  for t in tuples[1:]:
    intersection = intersection.intersection(set(t))

  return list(intersection)

def get_comprehensive_title(soup, url=None):
    title = None
    
    methods = [
        get_title_tag,
        get_og_title,
        get_twitter_title,
        get_schema_title,
        get_json_ld_title,
        get_h1_title,
        get_article_title,
        get_meta_title,
        get_heading_title,
    ]
    
    for method in methods:
        title = method(soup)
        if title:
            break
    
    if not title and url:
        title = extract_title_from_url(url)
    
    if title:
        title = clean_title(title)
    
    return title

def get_title_tag(soup):
    return soup.title.string if soup.title else None

def get_og_title(soup):
    og_title = soup.find("meta", property="og:title")
    return og_title["content"] if og_title else None

def get_twitter_title(soup):
    twitter_title = soup.find("meta", attrs={"name": "twitter:title"})
    return twitter_title["content"] if twitter_title else None

def get_schema_title(soup):
    schema_title = soup.find("meta", itemprop="headline")
    return schema_title["content"] if schema_title else None

def get_json_ld_title(soup):
    json_ld = soup.find("script", type="application/ld+json")
    if json_ld:
        import json
        try:
            data = json.loads(json_ld.string)
            return data.get("headline") or data.get("name")
        except json.JSONDecodeError:
            return None
    return None

def get_h1_title(soup):
    h1 = soup.find('h1')
    return h1.text.strip() if h1 else None

def get_article_title(soup):
    article_title = soup.find(class_=["post-title", "entry-title", "article-title", "blog-post-title"])
    return article_title.text.strip() if article_title else None

def get_meta_title(soup):
    meta_title = soup.find("meta", attrs={"name": "title"})
    return meta_title["content"] if meta_title else None

def get_heading_title(soup):
    for tag in ['h1', 'h2', 'h3']:
        heading = soup.find(tag)
        if heading:
            return heading.text.strip()
    return None

def extract_title_from_url(url):
    path = urlparse(url).path
    segments = [seg for seg in path.split('/') if seg]
    if segments:
        return segments[-1].replace('-', ' ').replace('_', ' ').capitalize()
    return None

def clean_title(title):
    title = ' '.join(title.split())
    
    common_suffixes = [" | Blog", " - Blog", " | Website Name", " - Website Name"]
    for suffix in common_suffixes:
        if title.endswith(suffix):
            title = title[:-len(suffix)]
    
    title = re.sub(r'&[a-zA-Z]+;', '', title)
    
    max_length = 100
    if len(title) > max_length:
        title = title[:max_length].rsplit(' ', 1)[0] + '...'
    
    return title.strip()

def get_content_from_url(url: str):
    try:
        response = requests.get(url)
        response.raise_for_status()  
    except requests.RequestException as e:
        raise ConnectionRefusedError(f"Error fetching the URL: {e}")

    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Get comprehensive title
    title = get_comprehensive_title(soup, url)
    
    # Get body text
    for script in soup(["script", "style"]):
        script.decompose()

    text = soup.get_text()
    lines = (line.strip() for line in text.splitlines())
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    body_text = '\n'.join(chunk for chunk in chunks if chunk)

    return title, body_text

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

# def LLM_Agent_for_title_desc (heads, content) :

#     url = "https://api.openai.com/v1/chat/completions"
    
#     headers = {
#         "Content-Type": "application/json",
#         "Authorization": f"Bearer {os.environ.get('OPENAI_API_KEY')}"
#     }
    
#     data = {
#         "model": "gpt-4o-mini-2024-07-18",
#         "messages": [
#             {
#                 "role": "system",
#                 "content": """You are a summarizer. Give the title or titles and the content, give a short and
#                               appropraite title of max 10 words, and a short and appropraite description of the 
#                               content in no more than 60 words. The output should be in JSON format, with a 
#                               title and a description attribute.
#                             """
#             },
#             {
#                 "role": "user",
#                 "content": f'Title: {heads}, Content: {content}'
#             }
#         ]
#     }
#     try :
#         response = requests.post(url, headers=headers, json=data)
#     except: 
#         raise Exception ("Error in getting the response")
    
#     try: 
#         res = response.json()['choices'][0]['message']['content']
#         res = json.loads(res[8:-4]) 
#     except: 
#         raise Exception("Error in parsing")
    
#     return res['title'], res['description']

