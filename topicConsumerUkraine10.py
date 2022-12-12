import os
from kafka3 import KafkaConsumer
from kafka3 import TopicPartition
from wordcloud import WordCloud
from cleanTweetSansPays import pre_process_tweet
import numpy as np
from PIL import Image
import json
import matplotlib.pyplot as plt
from alive_progress import alive_bar

#Variables globales
topic="ukraine20"
__location__ = os.path.realpath(
    os.path.join(os.getcwd(), os.path.dirname(__file__)))
partitions=[TopicPartition(topic, 0)]
total_sentences = ""

#Choix de l'image de base du wordcloud
twitter_mask = np.array(Image.open(os.path.join(__location__,"logo.jpg")))

# Choix du serveur kafka local ou distant
#listener = 'localhost:9092'
listener = '*****.ddns.net:9092'

#Définition des paramètres
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=[listener],
   auto_offset_reset='earliest'
)
last_offset_per_partition = consumer.end_offsets(partitions)

#choix du nombre de tweet à récupérer
repeat = 10000
#repeat = last_offset_per_partition[TopicPartition(topic, 0)]

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