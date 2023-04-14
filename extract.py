import requests
import time

LASTFM_API_KEY = '3f8f9f826bc4b0c8b529828839d38e4b'
DISCOGS_API_KEY = 'hhNKFVCSbBWJATBYMyIxxjCJDSuDZMBGnCapdhOy'

def extract_info_from_artist(artists_names):
    # initialize a list of dictionaries for each artist 
    artist_contents = []

    # extract for all artists' informations from last fm and store as a dict
    for name in artists_names:
        url = ('https://ws.audioscrobbler.com/2.0/?method=artist.getinfo&artist=') + name + (
            '&api_key=') + LASTFM_API_KEY + ('&format=json')
        artist_info = requests.get(url).json()
        artist_contents.append({'Artist': name, 'Content': artist_info['artist']['bio']['content']})
        print('Search infrmation for artist {} ...'.format(name))

    # return artist info for transform stage
    return artist_contents

def extract_titles_from_artist(name):
    # get the artist id from artist name
    url = ('https://api.discogs.com/database/search?q=') + name + ('&{?type=artist}&token=') + DISCOGS_API_KEY
    discogs_artist_info = requests.get(url).json()
    id = discogs_artist_info['results'][0]['id']

    print('Search releases from discogs.com for artist {} ...'.format(str(name)))

    # with id get artist's releases
    url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
    releases = requests.get(url).json()

    # store the releases/tracks info in a list
    releases_info = []
    for index in range(len(releases['releases'])):
            url = releases['releases'][index]['resource_url']
            source = requests.get(url).json()
            # search if exists track's price
            if 'lowest_price' in source.keys():  
                if 'formats' in source.keys():
                    releases_info.append({'Title': source['title'],
                                      'Collaborations': releases['releases'][index]['artist'],
                                      'Year': source['year'],
                                      'Format': source['formats'][0]['name'],
                                      'Discogs Price': source['lowest_price']})
                else:
                    releases_info.append({'Title': source['title'],
                                      'Collaborations': releases['releases'][index]['artist'],
                                      'Year': source['year'],
                                      'Format': None,
                                      'Discogs Price': source['lowest_price']})
                print('Found ' + str((index + 1)) + ' titles!')

            # sleep 3 secs to don't miss requests
            time.sleep(3)

    print('Found releases from artist ' + str(name) + ' with Discogs ID: ' + str(id))

    # return artist's tracks for transform stage
    return releases_info