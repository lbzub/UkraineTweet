from kafka3 import KafkaProducer
from kafka3 import KafkaConsumer
from kafka3 import TopicPartition
import tweepy
import datetime
import time
import json

# Choix du serveur kafka local ou distant
#listener = 'localhost:19092'
listener = 'emsst.ddns.net:9092'
topic = "ukraine20"
#Définitions des paramètres tweepy et kafka
client = tweepy.Client(bearer_token='AAAAAAAAAAAAAAAAAAAAAH03jQEAAAAAytapQlqUanV7mDeQ%2B386PKdQygM%3DdKev8v4TwXIuMfKaQ0AfAvxzxRlU9pziARLgR0CtVjoU2Z7Eu0')
producer = KafkaProducer(bootstrap_servers=[listener])


consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[listener],
   auto_offset_reset='earliest'
)
partitions=[TopicPartition(topic, 0)]
query = 'ukraine'
start_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=40)
end_time = datetime.datetime.utcnow() - datetime.timedelta(seconds=34)

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
                                         poll_fields=["duration_minutes",
                                                        "end_datetime",
                                                        "id",
                                                        "options",
                                                        "voting_status"],
                                         media_fields=["alt_text",
                                                        "duration_ms",
                                                        "height",
                                                        "media_key",
                                                        "non_public_metrics",
                                                        "organic_metrics",
                                                        "preview_image_url",
                                                        "promoted_metrics",
                                                        "public_metrics",
                                                        "type",
                                                        "url",
                                                        "variants",
                                                        "width"],
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
                                         expansions=["attachments.media_keys",
                                                    "attachments.poll_ids",
                                                    "author_id",
                                                    "edit_history_tweet_ids",
                                                    "entities.mentions.username",
                                                    "geo.place_id",
                                                    "in_reply_to_user_id",
                                                    "referenced_tweets.id",
                                                    "referenced_tweets.id.author_id"
                                                    ],
                                        max_results=10,
                                        start_time=start_time,
                                        end_time=end_time
                                        )


    start_time = end_time
    end_time = start_time + datetime.timedelta(seconds=6)

    if tweets is not None:
        for i,tweet in enumerate(tweets.data):
            lang = tweet.lang
            date = tweet.created_at
            data = tweet.data
            tweet = json.dumps(tweet.data).encode('utf-8')
            producer.send(topic, tweet)
            actualTweet +=1

            print(date, f'Le {actualTweet}ème tweet a été envoyé à Kafka!', 'langue', lang )

    timedate = datetime.datetime.now()
    last_offset_per_partition = consumer.end_offsets(partitions)
    totalTweet = last_offset_per_partition[TopicPartition(topic=topic, partition=0)]
    print(timedate.strftime("%H:%M:%S"), ' --> ' , actualTweet ,
          ' Total sur le TOPIC :', totalTweet , )
    time.sleep(6)