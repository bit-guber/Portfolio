
import undetected_chromedriver
import requests
from tqdm import tqdm
import json, os, gc, time


target_path = './raw_output'
target_episode_path = target_path + '/Episodes'
os.makedirs( target_path, exist_ok=True )
os.makedirs( target_episode_path, exist_ok=True )

general_headers =  {
  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36',
  'Accept': 'application/json, text/plain, */*',
  'Accept-Language': 'en-GB,en;q=0.9',
  'Accept-Encoding': 'gzip, deflate, br',
  'Referer': 'https://www.crunchyroll.com/videos/popular',
  
  'Authorization': 'Basic Y3Jfd2ViOg==',
  'Origin': 'https://www.crunchyroll.com',
  'DNT': '1',
  'Connection': 'keep-alive',
  'Sec-Ch-Ua': '"Not/A)Brand";v="99", "Google Chrome";v="115", "Chromium";v="115"',
  'Sec-Ch-Ua-Mobile': '?0',
  'Sec-Ch-Ua-Platform': '"Windows"',
  'Sec-Fetch-Dest': 'empty',
  'Sec-Fetch-Mode': 'cors',
  'Sec-Fetch-Site': 'same-origin',
}
cookies = dict()

def get_cookie(cookies):

    cookies['_dd_s'] = '&'.join( [x if 'expire' not in x else f'expire={ int((time.time()*1000) + 15*60*1000) }' for x in cookies['_dd_s'].split('&') ])
    cookies['_dd_s'] = '&'.join( [x if 'created' not in x else f'created={ int(time.time()*1000) }' for x in cookies['_dd_s'].split('&') ])
    return '; '.join( [ f'{k}={v}' for k,v in cookies.items() ] )
    

def get_auth(s,url):
    global general_headers,cookies

    driver = undetected_chromedriver.Chrome()

    driver.get(url)
    time.sleep(2)

    cookies = driver.get_cookies()

    cookies = dict()
    for i in driver.get_cookies():
        cookies[i['name']] = i['value']
    driver.close()
    driver.quit()

    auth_url = "https://www.crunchyroll.com/auth/v1/token"

    cookie = get_cookie(cookies)
    general_headers['Cookie'] = cookie
    general_headers['Authorization'] = 'Basic Y3Jfd2ViOg=='
    general_headers['Content-Type']= 'application/x-www-form-urlencoded'

    temp_id = [y for x,y in cookies.items() if x == "ajs_anonymous_id"]
    if len(temp_id)>0:
        general_headers['Etp-Anonymous-Id'] = temp_id[0]
    
    auth_payload = 'grant_type=client_id'
    auth_response = s.request("POST", auth_url, data=auth_payload, headers=general_headers)

    if general_headers.get( 'Etp-Anonymous-Id' ) is not None:
        del general_headers['Etp-Anonymous-Id']
    del general_headers['Content-Type']
    try:
        auth_token = auth_response.json()['access_token']
    except json.JSONDecodeError as e:
        print( "there is problem in get authucation part extract ", auth_response.status_code, general_headers,len(cookies),  cookies, url )
        raise ValueError("  ")   
    general_headers['Authorization'] = f"Bearer {auth_token}"
    return auth_token, cookies

def get_json( url, path, method = 'GET' , payload = None, headers = None, query = None ):
    global cookies, start_time,s
    if payload is None:
        payload = empty_payload
    if headers is None:
        headers = general_headers
    headers['Cookie'] = get_cookie( cookies )
    try:
        response = s.get( url, data=payload, headers=headers, params=query )
    except requests.ConnectionError :
        s = requests.Session()
        response = s.get( url, data=payload, headers=headers, params=query )
    try:
        json_object = response.json()
    except json.JSONDecodeError as e:   
        s = requests.Session()
        _1, _2 = get_auth(s,  general_headers['Referer']  )
        print( "login again " )
        response = s.get( url, data=payload, headers=general_headers, params=query )
        json_object = response.json()
    
    gc.collect()
    with open(path, 'w') as f:
        json.dump( json_object, f )
    
    return json_object
url = 'https://www.crunchyroll.com/videos/popular'

s = requests.Session()
auth_token, cookies = get_auth(s,url)
url = "https://www.crunchyroll.com/content/v2/discover/browse"

querystring = {"n":"1","sort_by":"popularity","ratings":"true","locale":"en-US"}

empty_payload = ""

response = s.get( url, data=empty_payload, headers=general_headers, params=querystring)
total_series_count = response.json()['total']

querystring['n'] = total_series_count

response = s.get(url, data=empty_payload, headers=general_headers, params=querystring)
popular_json = response.json()
with open(target_path + '/popular_list.json', 'w') as f:
    json.dump( popular_json, f )

print("total length series in popular list" , popular_json['data'].__len__() )

url = "https://www.crunchyroll.com/content/v2/discover/categories?locale=en-US"
lang_querystring = { 'locale': "en-US" }
response = s.get( url, data=empty_payload, headers=general_headers)
catelogies_json = response.json()
with open(target_path + '/categories.json', 'w') as f:
    json.dump( catelogies_json, f )

url = "https://static.crunchyroll.com/config/i18n/v3/audio_languages.json"
simple_headers = general_headers.copy()
del simple_headers['Cookie']
del simple_headers['Authorization']
simple_headers['Referer'] = simple_headers['Origin'] +'/'

response = s.get( url, data=empty_payload, headers=simple_headers)
audio_json = response.json()
with open(target_path + '/audio_list.json', 'w') as f:
    json.dump( audio_json, f )

print("total length series categories " , catelogies_json['data'].__len__() )

movie_count = 0
episodes_count = 0

for x in tqdm( popular_json['data'] ):
    if 'movie' in x['type']:
         movie_count+=1
         continue
    # time.sleep( 5 )
    series_id = x['id']
    
    slug_title = x['slug_title']
    general_headers['Referer'] = f'https://www.crunchyroll.com/series/{series_id}/{slug_title}'
    
    series_info = f'https://www.crunchyroll.com/content/v2/cms/series/{series_id}'
    path = f'{target_path}/series_des_{series_id}.json'

    series_json = get_json( series_info, path, query = lang_querystring )
    
    rating_url = f'https://www.crunchyroll.com/content-reviews/v2/rating/series/{series_id}'
    path = f'{target_path}/rating_{series_id}.json'
    
    rating_json = get_json(rating_url, path )

    music_url = f'https://www.crunchyroll.com/content/v2/music/featured/{series_id}'
    path = f'{target_path}/music_{series_id}.json'

    music_json = get_json( music_url, path, query = lang_querystring )
    
    season_url = f'https://www.crunchyroll.com/content/v2/cms/series/{series_id}/seasons'
    path = f'{target_path}/season_{series_id}.json'
    querystring = { 'force_locale':'', 'locale':'en-US' }

    season_json = get_json( season_url, path, query = querystring )
    
    for current_season in season_json['data']:
        # time.sleep(2)
        season_id = current_season['id']

        slug_title = x['slug_title']
        general_headers['Referer'] = f'https://www.crunchyroll.com/series/{series_id}/{slug_title}'
        
        episodes_url = f'https://www.crunchyroll.com/content/v2/cms/seasons/{season_id}/episodes'
        path = f'{target_episode_path}/episodes_{season_id}.json'

        episodes_json = get_json( episodes_url, path, query = lang_querystring )
        
        for scene in episodes_json['data']:
            episodes_count +=1
            episode_id = scene['id']
            
            slug_title = scene['slug_title']
            general_headers['Referer'] = f'https://www.crunchyroll.com/watch/{season_id}/{slug_title}'
            
            episode_rating = f'https://www.crunchyroll.com/content-reviews/v2/rating/episode/{episode_id}'
            path = f'{target_episode_path}/episode_rating_{episode_id}.json'

            episode_rating_json = get_json(episode_rating, path )
            
            comments_url = f'https://www.crunchyroll.com/talkbox/guestbooks'
            querystring = { 'guestbook_keys': episode_id, 'locale' : 'en-US' }
            path = f'{target_episode_path}/episode_commnets_{episode_id}.json'
            
            episode_comment_json = get_json( comments_url, path, query = querystring )
    gc.collect()
print("Movies count in crunchyroll", movie_count)
print("Episodes present in crunchyroll", episodes_count)

                     #   Total episodes count    +      total series meta-data   -   one folder count
req_per_sec=len(os.listdir(target_episode_path)) +  len(os.listdir(target_path)) - 1

print( f"How many requests took in a seconds? \n\t{req_per_sec}" )