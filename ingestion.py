import boto3
import random
import time
import json
from tweepy import API
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import configparser
from botocore.exceptions import ClientError
import os

config = configparser.ConfigParser()
config.read('auth_token.ini')

#twiiter api key
access_token = config['twitter']['access_token']
access_token_secret = config['twitter']['access_token_secret']
consumer_key = config['twitter']['consumer_key']
consumer_secret = config['twitter']['consumer_secret']

#firehose initiate
stream_name = 'tweet-stream'
os.environ['AWS_PROFILE'] = "personal_admin"
client = boto3.client('firehose', region_name='us-east-1')

#SNS initiate 
topic_arn = "arn:aws:sns:us-east-1:116403010356:Tweet_notification"
sns = boto3.client("sns")

#This is a basic listener that just prints received tweets and put them into the stream.
class MyStreamListener(StreamListener):
    def on_connect(self):
        print("Connected to the streaming server.")

    def on_status(self, status):
        print('Tweet text: ' + status.text)
        try:
            response = client.put_record(
                DeliveryStreamName=stream_name,
                Record={'Data': status.text})
            print(response)
            print("Uploaded to firehose\n")
        except ClientError as e:
            print("Error occurred: "+ e)
        return True

    def on_error(self, status):
        print(status)
        return False

# For following certain users
class StreamListenerFollow(StreamListener):
    def on_connect(self):
        print("Following users...")

    def on_status(self, status):
        print('Tweet text: ' + status.text)

        sub = f"Notification: New tweet from {status.user.name}"
        msg = f"A new tweet from {status.user.name} at {status.created_at}\
                \nTweet: {status.text}"
        try:
            response = sns.publish(
                TopicArn = topic_arn,
                Message = msg,
                Subject = sub
            )
            print(response)
            print("Notifications sent...\n")
        except ClientError as e:
            print("Error occurred: "+ e)
        return True
        
    def on_error(self, status):
        print(status)
        return False


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    myStreamListener = MyStreamListener()   
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = API(auth)
    #myStream = Stream(auth = api.auth, listener=myStreamListener)
    #myStream.filter(track=['#AWS','#glue','#sagemaker','#lambda','#EC2','#DynamoDB','#Comprehend','#Kinesis','#Athena', '#Redshift' ], is_async=True)

    Stream2 = Stream(auth = api.auth, listener=StreamListenerFollow())
    Stream2.filter(follow=['1275460974594723841'],is_async=True)