from kafka3 import KafkaConsumer
from kafka3 import TopicPartition
from wordcloud import WordCloud
from cleanTweet import pre_process_tweet
import numpy as np
from PIL import Image
import json
import matplotlib.pyplot as plt
from alive_progress import alive_bar

# Choix du serveur kafka local ou distant
#listener = 'localhost:19092'
listener = 'emsst.ddns.net:9092'

#Définition des paramètres
consumer = KafkaConsumer(
    "ukraine10",
    bootstrap_servers=[listener],
   auto_offset_reset='earliest'
)
total_sentences = ""
twitter_mask = np.array(Image.open("logo.jpg"))
partitions=[TopicPartition('ukraine10', 0)]
last_offset_per_partition = consumer.end_offsets(partitions)
repeat = last_offset_per_partition[TopicPartition(topic='ukraine10', partition=0)]

#Récupération des tweets sur le topic kafka
with alive_bar(repeat, force_tty=True) as bar:
    for i in range(repeat):
        tweet = json.loads(next(iter(consumer)).value)
        if tweet['lang'] == 'en':
            tweet = tweet ['text']
            clean_tweet = pre_process_tweet(tweet=tweet)
            total_sentences += clean_tweet
            total_sentences += " "
        #print(round(i / repeat * 100), "/100")
        bar()

#Création et affichage du wordcloud
wordcloud = WordCloud(collocations=False, width=800, height=500, random_state=42, max_font_size=100, mask=twitter_mask,
                      contour_color="steelblue", contour_width=0, background_color="white").generate(total_sentences)
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.show()