import json, os, gc, time, pandas as pd
from collections.abc import Iterable

start_time = time.time()


raw_target_path = './raw_output'
target_path = './output'
raw_target_episode_path = raw_target_path + '/Episodes'
os.makedirs( target_path, exist_ok=True )

# helping functions
def nested_iterable(iterable):
    if isinstance( iterable, (list, tuple) ):
        for i in iterable:
            if isinstance( i, (int, float, bool, str) ):
                return True
            elif isinstance( i, (list, tuple) ):
                return nested_iterable(i)
            else:
                return False
    return False
def combine_items(data):
    features = [k for k, v in data.items() if nested_iterable(v) ]
    for i in features:
        try:
            data[ i ] = ', '.join( data[ i ] )
        except:
            print(features, i, data)
            raise ValueError("Reinspection data flow ")
    return data

def remove_empty(data):
    temp = list( data.keys() )
    for k in temp:
        v = data[k]
        if isinstance( v, Iterable ):
            if len(v) == 0:
                del data[k] 
        elif isinstance( v, (bool, int, float) ) or v is None:
            continue
        else:
            print(v,type(v),k )
            raise ValueError("got irregular format")
    return data
def remove_singularity_cols(df):
    drop_list = []
    for c in df.columns:
        if len( df[c].astype(str).unique() ) == 1:
            drop_list.append(c)
    print( f'droping_list {drop_list}' )
    return df.drop(drop_list, axis = 1)



print( "start of popular" )
popular_json = dict()
with open(raw_target_path + '/popular_list.json', 'r') as f:
    popular_json = json.load( f )
for i in range(len(popular_json['data'])):
    for attr in [ 'linked_resource_key', 'images', 'last_public', 'external_id','slug_title', 'mature_blocked' ]:
        try:
            del popular_json['data'][i][attr]
        except KeyError:  
            pass
p = popular_json['data'].copy()

for _ in range(len(p)):
    try:
        for key, value in p[_]['series_metadata'].items():
            p[_][key] = value
        del p[_]['series_metadata']
    except KeyError as e:
        pass
    try:
        for key, value in p[_]['movie_listing_metadata'].items():
            p[_][key] = value
        del p[_]['movie_listing_metadata']
    except KeyError as e:
        pass
    
    p[_]['maturity_ratings'] = ','.join(p[_]['maturity_ratings'])
    
    p[_]['rating_average'] = p[_]['rating']['average']
    p[_]['rating_total'] = p[_]['rating']['total']
    del p[_]['rating']
    
    
    p[_] = remove_empty( p[_] )
    p[_] = combine_items( p[_] )


p = pd.DataFrame( p )
p = remove_singularity_cols(p)
p.to_csv( target_path + '/popular.csv', index = False )
del p
gc.collect()



print( "start of series " )
series = [ x for x in os.listdir(raw_target_path) if 'series_des_' in x and 'json' in x ]  
series_data = []
for x in series:
    series_json = dict()
    with open(raw_target_path + '/' + x, 'r') as f: 
        series_json = json.load( f )
        if len( series_json['data'] ) != 1:
            raise ValueError("not supported format ")
        series_json = remove_empty( series_json['data'][0] )
        series_json = combine_items( series_json )
        
        series_rating_json = dict()
        with open(raw_target_path + '/rating_' + series_json['id'] + '.json', 'r') as f: 
            series_rating_json = json.load( f )

        for i in range( 1, 6 ):
            for k, v in series_rating_json[ f'{i}s' ].items():
                series_rating_json[ f'{i}s_{k}' ] = v
            del series_rating_json[ f'{i}s' ]
        series_json.update( series_rating_json )
        
        for attr in ['mature_blocked', 'media_count', 'slug_title', 'availability_notes', 'seo_description', 'seo_title' ]:
            try:
                del series_json[attr]
            except KeyError as e:
                pass
        
        del series_json['images']
        series_data.append( series_json )
series_data = pd.DataFrame( series_data )
series_data = remove_singularity_cols( series_data )
series_data.to_csv( target_path + '/series.csv', index = False )
del series_data
gc.collect()



print( "start of series_music " )
series_music = [ x for x in os.listdir(raw_target_path) if 'music_' in x and 'json' in x ]  
series_music_data = []
for x in series_music:
    series_music_json = dict()
    with open(raw_target_path + '/' + x, 'r') as f: 
        series_music_json = json.load( f )
    for i in series_music_json['data']:
        i = remove_empty( i )
        
        i['startDate'] = i['availability']['startDate']
        i['genres'] = ', '.join( [_['displayValue'] for _ in  i['genres'] ])
        i['artist_name'] = i['artist']['name']
        i['artist_id'] = i['artist']['id']
        
        temp = i['animeIds'].copy()
        for attr in ['images','availability', 'streams_link', 'slug', 'artist', 'hash', "artists", 'animeIds' ]:
            del i[attr]
        
        for _ in temp:
            i['animeIds'] = _
            series_music_data.append( i.copy() )

series_music_data = pd.DataFrame( series_music_data )
series_music_data = remove_singularity_cols( series_music_data )
series_music_data = series_music_data.drop_duplicates(ignore_index=True)
series_music_data.to_csv( target_path + '/series_music.csv', index = False )

del series_music_data
gc.collect()


print( "start of audio_json" )
audio_json = dict()
with open(raw_target_path + '/audio_list.json', 'r') as f:
    audio_json = json.load( f )
    
with open(target_path + '/audio.json', 'w') as f:
    json.dump( audio_json, f )

del audio_json
gc.collect()


print( "start of categories_json")
catelogies_json = dict()
with open(raw_target_path + '/categories.json', 'r') as f:
    catelogies_json = {x['localization']['title'] : x['localization']['description'] for x in json.load( f )['data'] }
    
with open(target_path + '/categories.json', 'w') as f:
    json.dump( catelogies_json, f )
del catelogies_json
gc.collect()


print( "start of seasons "  )
season = [ x for x in os.listdir(raw_target_path) if 'season_' in x and 'json' in x ]  
season_data = []
for x in season:
    season_json = dict()
    with open(raw_target_path + '/' + x, 'r') as f: 
        season_json = json.load( f )
        for data in season_json['data']:
            data = remove_empty( data )
            data = combine_items( data )
            try:
                data['audio_versions'] = ', '.join([ i['audio_locale'] for i in data['versions']  ])
            except TypeError as e:
                data['audio_versions'] = ''
                
            for attr in [ 'mature_blocked','slug_title', 'versions' , "images", "extended_maturity_rating", "availability_notes", "audio_locales", "audio_locale"]:
                try: 
                    del data[attr]
                except KeyError:
                    pass
            season_data.append( data )
season_data = pd.DataFrame( season_data )
season_data = remove_singularity_cols( season_data )
season_data.to_csv( target_path + '/seasons.csv', index = False )
del season_data
gc.collect()


print( "start of episodes " )
episodes = [ x for x in os.listdir(raw_target_episode_path) if 'episodes_' in x and 'json' in x ]  
episodes_data = []
for x in episodes:
    episodes_json = dict()
    with open(raw_target_episode_path + '/' + x, 'r') as f: 
        episode_json = json.load( f )

        for data in episode_json['data']:
            data = remove_empty( data )
            data = combine_items( data )
            try:
                data['audio_versions'] = ', '.join([ i['audio_locale'] for i in data['versions']  ])
            except TypeError as e:
                data['audio_versions'] = ''
            try:
                data['ad_breaks_count'] = len( data['ad_breaks'] )
            except KeyError as e: 
                data['ad_breaks_count'] = 0
            for attr in [ 'slug_title', 'series_slug_title', 'streams_link', 'availability_notes', 'images', 'versions', 'season_slug_title']:
                try:
                    del data[attr]
                except KeyError as e:
                    pass
            episode_rating_json = dict()
            with open(raw_target_episode_path + '/episode_rating_' + data['id'] + '.json', 'r') as f: 
                episode_rating_json = json.load( f )
            
            episode_rating_json['rating_total'] = episode_rating_json['total']
            del episode_rating_json['total']
            for i in ['up', 'down']:
                for k, v in episode_rating_json[i].items():
                    episode_rating_json[ f'{i}_{k}' ] = v
                del episode_rating_json[ i ]
            data.update(episode_rating_json)
            
            episode_comments_json = dict()
            with open(raw_target_episode_path + '/episode_commnets_' + data['id'] + '.json', 'r') as f: 
                episode_comments_json = json.load( f )
            data['comments'] = episode_comments_json['items'][0]['total_comments']
            
            episodes_data.append( data )

episodes_data = pd.DataFrame( episodes_data )
episodes_data = remove_singularity_cols( episodes_data )
episodes_data.to_csv( target_path + '/episodes.csv', index = False )

del episodes_data
gc.collect()

print( f"{(time.time() - start_time)/60 } minutes took for completed tasks" )