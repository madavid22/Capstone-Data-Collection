import tweepy
import configparser
import pandas as pd
import csv
import geocoder

# read configs
config = configparser.ConfigParser()
config.read('config.ini')

api_key = config['twitter']['consumer_key']
api_key_secret = config['twitter']['consumer_secret']
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']

# authentication
auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)

api = tweepy.API(auth)

pd.set_option('display.max_rows', None)

most_popular_hashtags_year = ("""
    #love OR
    #Twitterers OR
    #smile OR
    #picofthedat OR
    #follow OR
    #fun OR
    #lol OR
    #friends OR
    #life OR
    #amazing OR
    #family OR
    #music
    """)

len(most_popular_hashtags_year)

polarizing_hashtags = (
    """
    #climatechange OR
    #globalwarming OR
    #hoax OR
    #BlackLivesMatter OR
    #BLM OR
    #ACAB OR
    #politics OR
    #unpopularopinion OR
    #opinion OR
    #biden OR
    #trump OR
    #ukraine OR
    #russia OR
    #china OR
    #putin OR
    #war OR
    #military OR
    #gay OR 
    #lesbian OR
    #gun OR
    #anti OR
    #cancel OR
    #inflation OR
    #gas OR
    #economy OR
    #unemployment
    """)
len(polarizing_hashtags)

# States Dictionary
states_map = {
            'AL': 'Alabama',
            'AK': 'Alaska',
            'AZ': 'Arizona',
            'AR': 'Arkansas',
            'CA': 'California',
            'CO': 'Colorado',
            'CT': 'Connecticut',
            'DE': 'Delaware',
            'DC': 'District of Columbia',
            'FL': 'Florida',
            'GA': 'Georgia',
            'HI': 'Hawaii',
            'ID': 'Idaho',
            'IL': 'Illinois',
            'IN': 'Indiana',
            'IA': 'Iowa',
            'KS': 'Kansas',
            'KY': 'Kentucky',
            'LA': 'Louisiana',
            'ME': 'Maine',
            'MD': 'Maryland',
            'MA': 'Massachusetts',
            'MI': 'Michigan',
            'MN': 'Minnesota',
            'MS': 'Mississippi',
            'MO': 'Missouri',
            'MT': 'Montana',
            'NE': 'Nebraska',
            'NV': 'Nevada',
            'NH': 'New Hampshire',
            'NJ': 'New Jersey',
            'NM': 'New Mexico',
            'NY': 'New York',
            'NC': 'North Carolina',
            'ND': 'North Dakota',
            'OH': 'Ohio',
            'OK': 'Oklahoma',
            'OR': 'Oregon',
            'PA': 'Pennsylvania',
            'RI': 'Rhode Island',
            'SC': 'South Carolina',
            'SD': 'South Dakota',
            'TN': 'Tennessee',
            'TX': 'Texas',
            'UT': 'Utah',
            'VT': 'Vermont',
            'VA': 'Virginia',
            'WA': 'Washington',
            'WV': 'West Virginia',
            'WI': 'Wisconsin',
            'WY': 'Wyoming'
         }

# max Tweets
limit = 100000000

# search in USA
places = api.search_geo(query = "USA", granularity = "country") #change to state
place_id = places[0].id

# query
tweets = tweepy.Cursor(api.search_tweets, q = "place:%s" % place_id + (polarizing_hashtags) + "-filter:retweets", lang = "en",
                      count = 100, tweet_mode = 'extended').items(limit)

# Build data frame
data = []

for tweet in tweets:
    data.append([tweet.user.location, tweet.full_text])

df = pd.DataFrame(data, columns=['location', 'status'])
    
df.head(), df.shape
df.shape

L = list(states_map.keys()) + list(states_map.values())

df['state'] = df['location'].str.extract('(' + '|'.join(L) + ')', expand=False).replace(states_map)

df = df.dropna(subset=['state'])

df.shape

df['state'].value_counts()
df[['state', 'status']].to_csv('tweets_with_states.csv')

df

# first attempt at extracting state from location but not really that helpful
def extract_state(location):
    try:
        location = location.split(', ')[-1]
    except AttributeError:
        return None
    if location.upper() in states_map:
        return states_map[location]
    else:
        return location
    
    
df['location'] = df['location'].apply(extract_state)

df