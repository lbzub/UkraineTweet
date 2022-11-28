from kafka3 import KafkaProducer
from kafka3 import KafkaConsumer
from kafka3 import TopicPartition
import tweepy
import datetime
import time
import json

# Choix du serveur kafka local ou distant
listener = 'localhost:19092'
#listener = 'emsst.ddns.net:9092'

#Définitions des paramètres tweepy et kafka
client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAH03jQEAAAAAytapQlqUanV7mDeQ%2B386PKdQygM%3DdKev8v4TwXIuMfKaQ0AfAvxzxRlU9pziARLgR0CtVjoU2Z7Eu0')
producer = KafkaProducer(bootstrap_servers=[listener])
consumer = KafkaConsumer(
    "ukraine10",
    bootstrap_servers=[listener],
   auto_offset_reset='earliest'
)
partitions=[TopicPartition('ukraine10', 0)]
query = 'ukraine'
start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=40)
end_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=34)

#Récupération des tweets et envoie en json sur le topic Kafka
while True:
    actualTweetWorld = 0
    actualTweet = 0
    tweets = client.search_recent_tweets(query=query,
                                        tweet_fields=['created_at', 'lang'],
                                        max_results=100,
                                        start_time=start_time,
                                        end_time=end_time
                                        )
    start_time = end_time
    end_time = start_time + datetime.timedelta(seconds=6)

    if tweets is not None:
        for i,tweet in enumerate(tweets.data):
            lang = tweet.lang
            date = tweet.created_at
            tweet = json.dumps(tweet.data).encode('utf-8')
            producer.send('ukraine10', tweet)
            actualTweet +=1

            print(date, f'Le {actualTweet}ème tweet a été envoyé à Kafka!', 'langue', lang )

    timedate = datetime.datetime.now()
    last_offset_per_partition = consumer.end_offsets(partitions)
    totalTweet = last_offset_per_partition[TopicPartition(topic='ukraine10', partition=0)]
    print(timedate.strftime("%H:%M:%S"), ' --> ' , actualTweet ,
          ' Total sur le TOPIC :', totalTweet , )
    time.sleep(6)