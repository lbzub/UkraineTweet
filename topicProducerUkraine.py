from kafka3 import KafkaProducer
from kafka3 import KafkaConsumer
from kafka3 import TopicPartition
import tweepy
import datetime
import time
import json

listener = 'localhost:19092'
#listener = 'emsst.ddns.net:9092'

client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAK03jQEAAAAAmnbCpXBMBxLqZcHjWC8pIySP%2B2w%3D5S15Kix46yNp1WdN10QjDwUSwfBLvnRUa2XjZ3hPZ0ce4cm36A')
producer = KafkaProducer(bootstrap_servers=[listener])
consumer = KafkaConsumer(
    "ukraine",
    bootstrap_servers=[listener],
   auto_offset_reset='earliest'
)

partitions=[TopicPartition('ukraine', 0)]
query = 'ukraine'
start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=40)
end_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=34)

while True:
    actualTweetWorld = 0
    actualTweet = 0

    tweets = client.search_recent_tweets(query=query,
                                        tweet_fields=['context_annotations', 'created_at', 'lang'],
                                        max_results=100,
                                        start_time=start_time,
                                        end_time=end_time
                                        )
    start_time = end_time
    end_time = start_time + datetime.timedelta(seconds=6)

    if tweets is not None:
        for i,tweet in enumerate(tweets.data):
            if tweet.lang == 'en':
                date = tweet.created_at
                tweet = json.dumps(tweet.text).encode('utf-8')
                producer.send('ukraine', tweet)
                actualTweet +=1
                print(date, f'Le {actualTweet}ème tweet a été envoyé à Kafka!')
            actualTweetWorld += 1
    timedate = datetime.datetime.now()
    last_offset_per_partition = consumer.end_offsets(partitions)
    totalTweet = last_offset_per_partition[TopicPartition(topic='ukraine', partition=0)]
    print(timedate.strftime("%H:%M:%S"), ' --> ' , actualTweet , 'Tweets anglais récupérés sur' , actualTweetWorld ,
          'Tweets mondiaux. Total sur le TOPIC :', totalTweet , )
    time.sleep(6)