from nltk.tokenize import RegexpTokenizer
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
import re
import string
import numpy

#test
def pre_process_tweet(tweet):
    # Remove any URL
    tweet = re.sub(r"http\S+", "", tweet)
    tweet = re.sub(r"www\S+", "", tweet)

    tokenizer = RegexpTokenizer(r'\w+')
    lemmatizer = WordNetLemmatizer()

    # tokenize and remove stop words and number
    tweet_tokens = tokenizer.tokenize(tweet)[1:]
    tweet_tokens = [word for word in tweet_tokens if word.isalpha()]
    tweet_tokens = [word for word in tweet_tokens if word.lower() != 'rt']
    tweet = " ".join([word for word in tweet_tokens if word not in stopwords.words('english')])
    tweet = " ".join([w for w in tweet_tokens if len(w) > 2])

    # remove \n from the end after every sentence
    tweet = tweet.strip('\n')

    # Remove any word that starts with the symbol @
    tweet = " ".join(filter(lambda x: x[0] != '@', tweet.split()))

    # remove non utf-8 characters
    tweet = bytes(tweet, 'utf-8').decode('utf-8', 'ignore')

    # remove colons from the end of the sentences (if any) after removing url
    tweet = tweet.strip()
    tweet_len = len(tweet)
    if tweet_len > 0:
        if tweet[len(tweet) - 1] == ':':
            tweet = tweet[:len(tweet) - 1]

    # Remove any hash-tags symbols
    tweet = tweet.replace('#', '')

    # Convert every word to lowercase
    tweet = tweet.lower()

    # remove punctuations
    tweet = tweet.translate(str.maketrans('', '', string.punctuation))

    # remove country
    tweet = tweet.replace('ukraine ', '')
    tweet = tweet.replace('ukrainian ', '')
    tweet = tweet.replace('russia ', '')
    tweet = tweet.replace('russian ', '')
    tweet = tweet.replace(' ukraine', '')
    tweet = tweet.replace(' ukrainian', '')
    tweet = tweet.replace(' russia', '')
    tweet = tweet.replace(' russian', '')


    # trim extra spaces
    tweet = " ".join(tweet.split())
    tweet = tweet.replace('  ', ' ')

    # lematize words
    tweet = lemmatizer.lemmatize(tweet)

    return (tweet)