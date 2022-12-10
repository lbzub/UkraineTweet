from kafka3 import KafkaProducer
from kafka3 import KafkaConsumer
from kafka3 import TopicPartition
from cleanTweet import pre_process_tweet
import tweepy
import datetime
from dateutil import tz
import time
import json
import os

# Choix du serveur kafka local ou distant
#listener = 'localhost:19092'
listener = 'emsst.ddns.net:9092'
topic = "ukraine20"
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
to_zone = tz.tzlocal()
#Définitions des paramètres tweepy et kafka
client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAH03jQEAAAAAytapQlqUanV7mDeQ%2B386PKdQygM%3DdKev8v4TwXIuMfKaQ0AfAvxzxRlU9pziARLgR0CtVjoU2Z7Eu0',
                       wait_on_rate_limit=True)
# bearer_token de secours : AAAAAAAAAAAAAAAAAAAAAJwEkQEAAAAA0LJ9ZzVMVBlh8PbsjOqBNZFsBGY%3DIm5qzbE8nHVfAPvcSOohWykwWw3SfG7Nx18Ur3XBD4ySZcKiQ7
producer = KafkaProducer(bootstrap_servers=[listener])
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[listener],
   auto_offset_reset='earliest'
)
partitions=[TopicPartition(topic, 0)]
query = 'ukraine'
start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=40)
end_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=30)


geoJson = open(os.path.join(__location__, 'ua.geojson'))
villes= json.load(geoJson)


#Récupération des tweets et envoie en json sur le topic Kafka
while True:
    actualTweetWorld = 0
    actualTweet = 0
    tweets = client.search_recent_tweets(query=query,
                                         tweet_fields=["attachments",
                                                        "author_id",
                                                        "context_annotations",
                                                        "conversation_id",
                                                        "created_at",
                                                        "edit_history_tweet_ids",
                                                        "entities",
                                                        "geo",
                                                        "id",
                                                        "in_reply_to_user_id",
                                                        "lang",
                                                        "possibly_sensitive",
                                                        "public_metrics",
                                                        "referenced_tweets",
                                                        "reply_settings",
                                                        "source",
                                                        "text",
                                                        "withheld"],
                                         #poll_fields=["duration_minutes",
                                                      #  "end_datetime",
                                                       # "id",
                                                       # "options",
                                                       # "voting_status"],
                                         # media_fields=["alt_text",
                                         #                "duration_ms",
                                         #                "height",
                                         #                "media_key",
                                         #                "non_public_metrics",
                                         #                "organic_metrics",
                                         #                "preview_image_url",
                                         #                "promoted_metrics",
                                         #                "public_metrics",
                                         #                "type",
                                         #                "url",
                                         #                "variants",
                                         #                "width"],
                                         user_fields=["created_at",
                                                    "description",
                                                    "entities",
                                                    "id",
                                                    "location",
                                                    "name",
                                                    "pinned_tweet_id",
                                                    "profile_image_url",
                                                    "protected",
                                                    "public_metrics",
                                                    "url",
                                                    "username",
                                                    "verified",
                                                    "withheld"],
                                         place_fields=["contained_within",
                                                        "country",
                                                        "country_code",
                                                        "full_name",
                                                        "geo",
                                                        "id",
                                                        "name",
                                                        "place_type"],
                                         expansions=[
                                                    "author_id",
                                                    "edit_history_tweet_ids",
                                                    "entities.mentions.username",
                                                    "geo.place_id",
                                                    "in_reply_to_user_id",
                                                    "referenced_tweets.id",
                                                    "referenced_tweets.id.author_id"
                                                    ],
                                        max_results=100,
                                        start_time=start_time,
                                        end_time=end_time
                                        )


    start_time = end_time
    end_time = start_time + datetime.timedelta(seconds=10)



    if tweets.data is not None:
        dateFirstTweet = ''
        for i,tweet in enumerate(tweets.data):
            userCreateDate = ''
            userLocation = ''
            userName = ''
            displayName = ''
            userVerified = ''
            villesTweet = []
            tweetTexte = tweet.text
            clean_tweet = pre_process_tweet(tweet=tweetTexte)
            words = clean_tweet.split()
            for word in words:
                for ville in villes['features']:
                    if ville['properties']['name'].lower() == word.lower():
                        villePoint = {}
                        villePoint['text'] = ville['properties']['name']
                        villePoint['location'] = ville['geometry']
                        villesTweet.append(villePoint)

                        # "text": "Geopoint as an object using GeoJSON format",
                        # "location": {
                        #     "type": "Point",
                        #     "coordinates": [-71.34, 41.12]

            for i, user in enumerate(tweets.includes.get('users')):
                if user.id == tweet.author_id:
                    userCreateDate = user.created_at
                    userLocation = user.location
                    userName = user.username
                    displayName = user.name
                    userVerified = user.verified
            tw ={}
            tw['lang'] = tweet.lang
            tw['date'] = tweet.created_at.strftime("%Y-%m-%d"'T'"%H:%M:%S")
            tw['text'] = tweet.text
            tw['villesTweet'] = villesTweet
            tw['contextAnnotation'] = tweet.context_annotations
            tw['source'] = tweet.source
            tw['userCreateDate'] = userCreateDate.strftime("%Y-%m-%d"'T'"%H:%M:%S")
            tw['userName'] = userName
            tw['displayName'] = displayName
            tw['userLocation'] = userLocation
            tw['userVerified'] = userVerified


            #print(tw)

            tw = json.dumps(tw).encode('utf-8')


            producer.send(topic, tw)
            actualTweet +=1

            print(tweet.created_at, f'Le {actualTweet}ème tweet a été envoyé à Kafka!', 'langue', tweet.lang )
            if dateFirstTweet == '' :
                dateFirstTweet = tweet.created_at.astimezone(to_zone).strftime("%H:%M:%S")

    timedate = datetime.datetime.now().strftime("%H:%M:%S")
    last_offset_per_partition = consumer.end_offsets(partitions)
    totalTweet = last_offset_per_partition[TopicPartition(topic=topic, partition=0)]
    d1 = datetime.datetime.strptime(timedate,"%H:%M:%S")
    d2 = datetime.datetime.strptime(dateFirstTweet,"%H:%M:%S")
    tempsDiffere = abs((d2 - d1).total_seconds())

    print(timedate, ' --> ' , actualTweet ,
          ' Total sur le TOPIC :', totalTweet , tempsDiffere )

    if tempsDiffere <= 30 :
        time.sleep(6)
    time.sleep(4)