# import python libraries
import os, json, tweepy, boto3, geocoder
from textblob import TextBlob
from dotenv import load_dotenv

# load .ENV file
load_dotenv()

# aws SDK client
client = boto3.client('firehose',
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

# authenticate twitter api
auth = tweepy.OAuthHandler(os.getenv("TWITTER_API_KEY"), os.getenv("TWITTER_API_SECRET"))
auth.set_access_token(os.getenv("TWITTER_TOKEN"), os.getenv("TWITTER_TOKEN_SECRET"))
api = tweepy.API(auth)

# kinesis firehose configurations
KINESIS_STREAM_NAME = os.getenv("KINESIS_STREAM_NAME")

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
                    lat = result.json['raw']['lat']
                    lon = result.json['raw']['lon']
                    country = result.json['raw']['address']['country']
                    data = {'latitude': lat, 'longitude': lon, 'country': country}
                    return data
                else:
                    return None
            except Exception:
                return None
        else:
            return None

    
    def process_event(self, status):
        # process event
        event = {}
        sentiment = self.sentiment_analysis(status.text)
        location = self.get_location_data(status.user.location)
        if status.lang == 'en' and location != None and sentiment != None:
            event['_id'] = status.id
            event['text'] = status.text
            event['user'] = status.user.screen_name
            event['sentiment'] = self.sentiment_analysis(status.text)
            event['latitude'] = location['latitude']
            event['longitude'] = location['longitude']
            event['country'] = location['country']
            event['created_at'] = (status.created_at).strftime('%Y-%m-%d %H:%M:%S')
            
            # push event to kinesis firehose
            client.put_record(
                DeliveryStreamName=os.getenv("KINESIS_STREAM_NAME"),
                Record={
                    'Data': json.dumps(event)
                }
            )

            print(event)


    def on_status(self, status):
        # process tweets on arrival
        self.process_event(status)
        return True


    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False 


# create a stream
myStreamListener = MyStreamListener()
myStream = tweepy.Stream(auth = api.auth, listener=myStreamListener)

# keywords to look for in tweets
keywords = ['corona', 'covid', 'coronavirus', 'covid19', 'covid vaccine']

# start the stream
myStream.filter(track=keywords, is_async=True)