# import python libraries
import os, tweepy, geocoder
from textblob import TextBlob
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta

# load .ENV file
load_dotenv()

# authenticate twitter api
auth = tweepy.OAuthHandler(os.getenv("TWITTER_API_KEY"), os.getenv("TWITTER_API_SECRET"))
auth.set_access_token(os.getenv("TWITTER_TOKEN"), os.getenv("TWITTER_TOKEN_SECRET"))
api = tweepy.API(auth)

# connect to ElasticSearch
es = Elasticsearch([{'host': os.getenv("ES_HOST"), 'port': int(os.getenv("ES_PORT"))}])

# create a stream listener
class MyStreamListener(tweepy.StreamListener):
    def sentiment_analysis(self, text):
        # perform sentiment analysis
        try:
            polarity_score = TextBlob(text).sentiment.polarity
            if polarity_score < 0:
                return 'Negative'
            elif polarity_score == 0:
                return 'Neutral'
            else:
                return 'Positive'
        except Exception:
            return None            
    

    def get_location_data(self, location):
        # geocode users location
        if location != None:
            try:
                result = geocoder.osm(location)
                if result.ok:
                    lat = float(result.json['raw']['lat'])
                    lon = float(result.json['raw']['lon'])
                    country = result.json['raw']['address']['country']
                    country_iso2 = result.json['raw']['address']['country_code'].upper()
                    data = {'latitude': lat, 'longitude': lon, 'country': country, 'country_iso2': country_iso2}
                    return data
                else:
                    return None
            except Exception:
                return None
        else:
            return None

    
    def process_event(self, status):
        """This is called when raw data is received from the stream.
        This method handles sending the data to other methods, depending on the
        message type.
        """
        event = {}
        sentiment = self.sentiment_analysis(status.text)
        location = self.get_location_data(status.user.location)
        if status.lang == 'en' and location != None and sentiment != None:
            event['uid'] = status.id
            event['text'] = status.text
            event['user'] = status.user.screen_name
            event['sentiment'] = self.sentiment_analysis(status.text)
            event['country'] = location['country']
            event['country_iso2'] = location['country_iso2']
            event['geo'] = { 'location': str(location['latitude']) + "," + str(location['longitude']) }
            event['@timestamp'] = datetime.now() - timedelta(minutes=60)

            # insert data to elasticsearch index
            es.index(index='tweet_streamer', id=event['uid'], body=event)
            print(event)


    def on_status(self, status):
        # process tweets on arrival
        self.process_event(status)
        return True


    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False 


def main():
    # create a stream
    myStreamListener = MyStreamListener()
    myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

    # create elasticsearch index
    try:
        body = {
            "mappings": {
                "properties": {
                    "geo": {
                        "properties": {
                            "location": {
                                "type": "geo_point"
                            }
                        }
                    }
                }
            }
        }
        es.indices.create(index='tweet_streamer', body=body)
    except Exception:
        pass


    # start the stream
    while True:
        try:
            # keywords to look for in tweets
            keywords = ['corona', 'covid', 'coronavirus', 'covid19', 'covid vaccine']
            myStream.filter(track=keywords, stall_warnings=True)
        except Exception:
            print("Unable to start streaming...")
            continue


if __name__ == '__main__':
    main()