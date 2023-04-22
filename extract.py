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

    # with id get artist's releases
    url = ('https://api.discogs.com/artists/') + str(id) + ('/releases')
    
    releases = requests.get(url).json()

    print('Found releases from discogs.com for artist ' + str(name) + ' with Discogs ID: ' + str(id))
  
    return releases['releases']

def find_info_for_titles(releases: dict):
  # store the releases/tracks info in a list
  releases_info = []
  for index in range(len(releases)):

    url = releases[index]['resource_url']
    source = requests.get(url).json()
    # search if exists track's price
    if 'lowest_price' in source.keys():  
      if 'formats' in source.keys():
        releases_info.append({'Title': source['title'],
                              'Collaborations': releases[index]['artist'],
                              'Year': source['year'],
                              'Format': source['formats'][0]['name'],
                              'Discogs Price': source['lowest_price']})
      else:
        releases_info.append({'Title': source['title'],
                              'Collaborations': releases[index]['artist'],
                              'Year': source['year'],
                              'Format': None,
                              'Discogs Price': source['lowest_price']})
    print('Found informations from discogs.com for {} titles'.format(str((index + 1))))
    # sleep 3 secs to don't miss requests
    time.sleep(3)

  # return artist's tracks for transform stage
  return releases_info

def extract_playcounts_from_titles_by_artist(name: str, releases: dict):
  # initialize list for playcounts for each title
  playcounts = []
  # find playcounts from lastfm for each release title
  for index in range(len(releases)):
    title = releases[index]['title']
    url = 'https://ws.audioscrobbler.com/2.0/?method=track.getInfo&api_key=' + LASTFM_API_KEY + '&artist=' + name + '&track='+ title + '&format=json'

    try:
      source = requests.get(url).json()
      if 'track' in source.keys():
        playcounts.append({'Title': source['track']['name'],
                          'Lastfm Playcount': source['track']['playcount']})
        print('Found playcount from last.fm for title {}'.format(title))
      else:
        print('Not found playcount from last.fm for title {}'.format(title))
    except:
      print('Not found playcount from last.fm for title {}'.format(title))
      continue
  
  return playcounts