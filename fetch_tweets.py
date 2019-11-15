import json
import os
import ratelimiter
import twython

from absl import app
from absl import flags
from absl import logging

FLAGS = flags.FLAGS

flags.DEFINE_string('input_tweet_ids_file', None, 'File containing on id per line.')

flags.DEFINE_string('output_tweets_directory', None, 'Directory containing the tweet data.')

flags.DEFINE_integer('output_tweets_file_size', 1000, 'Number of tweets per output file.')

flags.DEFINE_integer('rate_limit', 1, 'Maximum fetches per second.')


CONSUMER_KEY = 'hidden'
CONSUMER_SECRET = 'hidden'
OAUTH_TOKEN = 'hidden'
OAUTH_TOKEN_SECRET = 'hidden'


def GetNextBasename(directory):
    if not os.path.isdir(directory):
        logging.fatal('Not a valid directory: ' + directory)

    index = 0
    while True:
        basename = str(index) + '.json'
        if not os.path.exists(os.path.join(directory, basename)):
            return basename

        index = index + 1


def main(unused):
    twitter = twython.Twython(CONSUMER_KEY, CONSUMER_SECRET, OAUTH_TOKEN, OAUTH_TOKEN_SECRET)
    if not twitter:
        logging.fatal('Invalid twitter credentials!')

    if not FLAGS.input_tweet_ids_file:
        logging.fatal('Must specify --input_tweet_ids_file!')

    if not FLAGS.output_tweets_directory:
        logging.fatal('Must specify --output_tweets_directory!')
        
    if not os.path.isdir(FLAGS.output_tweets_directory):
        os.makedirs(FLAGS.output_tweets_directory)

    # Prevents us from sending too many requests to Twitter too quickly.
    limiter = ratelimiter.RateLimiter(max_calls=FLAGS.rate_limit, period=1.5)

    # Fetches a single Tweet at a time.
    def GetTweet(id):
        with limiter:
            return twitter.show_status(id=id)
    
    # Fetches up to 100 Tweets at a time.
    def GetTweets(ids):
        if len(ids) > 100:
            logging.fatal('Max 100 ids per batch lookup')

        combined_ids = ','.join(ids)

        with limiter:
            return twitter.lookup_status(id=combined_ids)

    # Maps tweet id to the actual data of the tweet (text, timestamp, etc).
    tweet_id_to_tweet_data = dict()
    
    # Maps tweet id to which file the actual data is in.
    tweet_id_to_tweet_data_filename = dict()

    tweet_mapping_filename = os.path.join(FLAGS.output_tweets_directory, 'tweet_mapping.json')
    if os.path.exists(tweet_mapping_filename):
        with open(tweet_mapping_filename, 'r') as tweet_mapping_file:
            tweet_id_to_tweet_data_filename = json.load(tweet_mapping_file)

    with open(FLAGS.input_tweet_ids_file, 'r') as input_tweet_ids_file:
        tweet_ids_to_fetch = []
        for tweet_id in input_tweet_ids_file:
            tweet_id = tweet_id.strip()
        
            # Already fetched this Tweet before, don't do it again.
            if tweet_id in tweet_id_to_tweet_data_filename:
                logging.info('Skipping fetch tweet ' + tweet_id)
                continue
            
            # Add this Tweet to the batch of Tweets to lookup next.
            tweet_ids_to_fetch.append(tweet_id)
            
            # Lookup in batches of 100
            if len(tweet_ids_to_fetch) < 100:
                continue

            logging.info('Fetching batch of tweets...')

            while True:
                try:
                    tweet_datas = GetTweets(tweet_ids_to_fetch)
                    for tweet_data in tweet_datas:
                        tweet_id = tweet_data['id_str']
                        tweet_id_to_tweet_data[tweet_id] = tweet_data
                    
                    # Mark that we've already tried to fetch failures.
                    for tweet_id_to_fetch in tweet_ids_to_fetch:
                        if tweet_id_to_fetch not in tweet_id_to_tweet_data:
                            tweet_id_to_tweet_data[tweet_id_to_fetch] = {}

                    tweet_ids_to_fetch = []
                    break
                except twython.TwythonRateLimitError as err:
                    logging.info(str(err) + ' ... trying again')
                    continue
                except twython.TwythonError as err:
                    logging.fatal(err)

            # Dump the Tweets to a file in batches.
            if len(tweet_id_to_tweet_data) >= FLAGS.output_tweets_file_size:
                tweet_data_basename = GetNextBasename(FLAGS.output_tweets_directory)
                tweet_data_filename = os.path.join(FLAGS.output_tweets_directory, tweet_data_basename)
                with open(tweet_data_filename, 'w') as tweet_data_file:
                    json.dump(tweet_id_to_tweet_data, tweet_data_file)

                for tweet_id in tweet_id_to_tweet_data:
                    tweet_id_to_tweet_data_filename[tweet_id] = tweet_data_basename

                with open(tweet_mapping_filename, 'w') as tweet_mapping_file:
                    json.dump(tweet_id_to_tweet_data_filename, tweet_mapping_file)
                
                tweet_id_to_tweet_data = dict()

if __name__ == '__main__':
  app.run(main)