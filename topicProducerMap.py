from kafka3 import KafkaProducer
from kafka3 import KafkaConsumer
from kafka3 import TopicPartition
import tweepy
import datetime
import time
import json
from cleanTweet import pre_process_tweet
from alive_progress import alive_bar
import os

# Choix du serveur kafka local ou distant
#listener = 'localhost:19092'
listener = 'emsst.ddns.net:9092'
topic = 'ukraine10'
partitions=[TopicPartition(topic, 0)]
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
#
# producer = KafkaProducer(bootstrap_servers=[listener])
consumer = KafkaConsumer(
     topic ,
     bootstrap_servers=[listener],
    auto_offset_reset='earliest'
 )



#Choix du  nombre de tweets dans lesquels chercher les villes
#last_offset_per_partition = consumer.end_offsets(partitions)
#repeat = last_offset_per_partition[TopicPartition(topic= topic, partition=0)]
repeat = 5000

geoJson = open(os.path.join(__location__, 'ua.geojson'))
villes= json.load(geoJson)
villesTweet = []

with alive_bar(repeat, force_tty=True) as bar:
    for i in range(repeat):
        #print(i)
        tweet = json.loads(next(iter(consumer)).value)
        tweet = tweet['text']
        clean_tweet = pre_process_tweet(tweet=tweet)
        words = clean_tweet.split()
        for word in words:
            for ville in villes['features']:
                if ville['properties']['name'].lower() == word.lower():
                    villesTweet.append(ville['properties']['name'])
        bar()


for x in range(len(villesTweet)):
    print(villesTweet[x])

print(villesTweet.__len__(),"villes sont mentionnées dans",repeat,"tweets")



    # if tweets is not None:
    #     for i,tweet in enumerate(tweets.data):
    #         lang = tweet.lang
    #         date = tweet.created_at
    #         tweet = json.dumps(tweet.data).encode('utf-8')
    #         producer.send('map', tweet)
    #         actualTweet +=1
    #
    #         print(date, f'Le {actualTweet}ème tweet a été envoyé à Kafka!', 'langue', lang )
    #
    # timedate = datetime.datetime.now()
    # last_offset_per_partition = consumer.end_offsets(partitions)
    # totalTweet = last_offset_per_partition[TopicPartition(topic='map', partition=0)]
    # print(timedate.strftime("%H:%M:%S"), ' --> ' , actualTweet ,
    #       ' Total sur le TOPIC :', totalTweet , )
    # time.sleep(6)