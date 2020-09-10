# This script is designed to scrape data from reddit.

import datetime
import sys

import pandas as pd
import praw
import requests

import process_text
from words import WSB_DICT

reddit = praw.Reddit(client_id='ZYE9ySYFHYrATA', client_secret='2iAR5Sit-eDkGVBazvDmNwL6XmU', user_agent='WSB Sentiment Analysis')

# Now get the 50 hottest posts on the wallstreetbets subreddit.

wsb = reddit.subreddit('wallstreetbets')



# Updates the sentiment dictionary given a cleaned_text.
def sentiment_helper(text, sentiment_dict):
    try:
        cleaned_text = process_text.clean_text(text)
        # First, calculate the sentiment.
        # sentiment = process_text.sentiment_analyse(cleaned_text)
        # sentiment_dict['positive'] += sentiment['pos']
        # sentiment_dict['negative'] += sentiment['neg']
        # Now, calculate the bullish/bearish word frequency.
        tokens = process_text.get_final_words(process_text.tokenize_words(cleaned_text))
        for word in tokens.keys():
            if word in WSB_DICT:
                sentiment_dict[WSB_DICT[word]] += tokens[word] # Increase the bull/bear count by thr word frequency.
    except:
        pass

# Calculate sentiment from a dataframe of hot posts.
def calculate_sentiment(df):
    print(df)

    titles_sentiment_dict = {'bull': 0, 'bear': 0, 'positive': 0, 'negative': 0}
    body_sentiment_dict = {'bull': 0, 'bear': 0, 'positive': 0, 'negative': 0}
    comments_sentiment_dict = {'bull': 0, 'bear': 0, 'positive': 0, 'negative': 0}

    for index, row in df.iterrows():


        # Extract information from the title.
        try:
            title = row['Title']
            sentiment_helper(title, titles_sentiment_dict)
        except:
            sys.stderr.write("Title could not be cleaned.\n")


        try:
            body = row['Body']
            sentiment_helper(body, body_sentiment_dict)
        except:
            sys.stderr.write("Body could not be cleaned.\n")

        try:
            submission = reddit.submission(url=row['URL']) # Create a submission object.
            submission.comments.replace_more(limit=1) # When set to 0, replace_more will remove all MoreComments.
            for comment in submission.comments.list():
                try:
                    content = comment.body
                    sentiment_helper(content, comments_sentiment_dict)

                except:
                    pass

        except:
            sys.stderr.write("Comments could not be cleaned.\n")

        print(index)


    print(titles_sentiment_dict)
    print(body_sentiment_dict)
    print(comments_sentiment_dict)


def get_date_N_days_ago(n):
    date_N_days_ago = datetime.datetime.now() - datetime.timedelta(days=n)
    # TODO: check if the .replace changes in place or not.
    date_N_days_ago = date_N_days_ago.replace(hour=0, minute=0, second=0, microsecond=0)
    # Convert into a timestamp.
    return int(date_N_days_ago.timestamp())


def get_posts_for_time_period(sub, beginning, end=int(datetime.datetime.now().timestamp())):
    """
    Gets posts from the given subreddit for the given time period
    :param sub: the subreddit to retrieve posts from
    :param beginning: The unix timestamp of when the posts should begin
    :param end: The unix timestamp of when the posts should end (defaults to right now)
    :return:
    """
    print("Querying pushshift... this may take about a minute.")
    url = "https://apiv2.pushshift.io/reddit/submission/search/" \
          "?subreddit={0}" \
          "&sort=desc" \
          "&sort_type=num_comments" \
          "&limit=25" \
          "&after={1}" \
          "&before={2}".format(sub, beginning, end)

    response = requests.get(url)
    resp_json = response.json()
    return resp_json['data']

def get_sentiment_for_day(date_str):
    date = datetime.datetime.strptime(date_str, '%Y-%m-%d')
    beginning_timestamp = int(date.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
    end_timestamp = int((date + datetime.timedelta(days=1)).timestamp())
    print('Pulling WSB sentiment data from ' + date_str)
    # Get the 25 most commented posts from day_count days ago.
    data = get_posts_for_time_period("wallstreetbets", beginning_timestamp, end_timestamp)

    titles_sentiment_dict = {'bull': 0, 'bear': 0, 'positive': 0, 'negative': 0}
    body_sentiment_dict = {'bull': 0, 'bear': 0, 'positive': 0, 'negative': 0}
    comments_sentiment_dict = {'bull': 0, 'bear': 0, 'positive': 0, 'negative': 0}

    for post in data:
        submission = reddit.submission(id=post['id'])
        # Get information from the comments.
        try:
            submission.comments.replace_more(
                limit=0)  # When set to 0, replace_more will remove all MoreComments
            comment_list = submission.comments.list()
            for comment in comment_list:
                try:
                    content = comment.body
                    sentiment_helper(content, comments_sentiment_dict)
                except:
                    pass
        except Exception as e:
            sys.stderr.write(e)
            sys.stderr.write("Comments could not be cleaned.\n")

        print(comments_sentiment_dict)

        # This is used for retrieving yesterday's sentiment data.
        bull = comments_sentiment_dict['bull']
        bear = comments_sentiment_dict['bear']
        try:
            bull_bear_ratio = bull/bear
        except:
            bull_bear_ratio = 0.0
        print('Finished pulling WSB data on day ' + date_str)
        return {
            'Date': date_str,
            'Bull Comments': bull,
            'Bear Comments': bear,
            'Bull/Bear Ratio': bull_bear_ratio
        }



def get_sentiment_for_time_period(period_length_in_days, to_csv=False):
    day_count = period_length_in_days
    beginning_timestamp = get_date_N_days_ago(day_count)
    end_timestamp = get_date_N_days_ago(day_count-1)

    titles = []
    bodies = []
    comments = []

    while day_count >= 1: # We want the end_timestamp to be at least 1 day ago, in order to make predictions.

        print('Pulling data from ' + str(day_count) + ' days ago.')
        # Get the 25 most commented posts from day_count days ago.
        data = get_posts_for_time_period("wallstreetbets", beginning_timestamp, end_timestamp)

        titles_sentiment_dict = {'bull': 0, 'bear': 0, 'positive': 0, 'negative': 0}
        body_sentiment_dict = {'bull': 0, 'bear': 0, 'positive': 0, 'negative': 0}
        comments_sentiment_dict = {'bull': 0, 'bear': 0, 'positive': 0, 'negative': 0}

        for post in data:
            submission = reddit.submission(id=post['id'])

            # Get information from the titles.
            try:
                title = submission.title
                sentiment_helper(title, titles_sentiment_dict)
            except:
                sys.stderr.write("Title could not be cleaned.\n")

            # Get information from the post.
            try:
                body = submission.selftext
                sentiment_helper(body, body_sentiment_dict)
            except:
                sys.stderr.write("Body could not be cleaned.\n")

            # Get information from the comments.
            try:
                submission.comments.replace_more(
                    limit=0)  # When set to 0, replace_more will remove all MoreComments.
                for comment in submission.comments.list():
                    try:
                        content = comment.body
                        sentiment_helper(content, comments_sentiment_dict)
                    except:
                        pass
            except:
                sys.stderr.write("Comments could not be cleaned.\n")

        # Accumulate the data.
        print('Read posts from ' + str(day_count) + ' days ago.')

        historical_date = datetime.datetime.fromtimestamp(beginning_timestamp).strftime('%Y-%m-%d')
        titles.append([historical_date, titles_sentiment_dict['bull'], titles_sentiment_dict['bear'],
                       titles_sentiment_dict['positive'], titles_sentiment_dict['negative']])
        bodies.append([historical_date, body_sentiment_dict['bull'], body_sentiment_dict['bear'],
                       body_sentiment_dict['positive'], body_sentiment_dict['negative']])
        comments.append([historical_date, comments_sentiment_dict['bull'], comments_sentiment_dict['bear'],
                       comments_sentiment_dict['positive'], comments_sentiment_dict['negative']])

        # print(titles_sentiment_dict)
        # print(body_sentiment_dict)
        # print(comments_sentiment_dict)

        beginning_timestamp = end_timestamp
        day_count -= 1
        end_timestamp = get_date_N_days_ago(day_count-1)

    # Accumulate the data in a dataframe.
    df = {}
    df['title'] = pd.DataFrame(columns=['Date', 'Bull', 'Bear', 'Positive', 'Negative'],
                                data=titles).set_index('Date')
    df['body'] = pd.DataFrame(columns=['Date', 'Bull', 'Bear', 'Positive', 'Negative'],
                                data=bodies).set_index('Date')
    df['comments'] = pd.DataFrame(columns=['Date', 'Bull', 'Bear', 'Positive', 'Negative'],
                                data=comments).set_index('Date')
    df = pd.concat(df, axis=1)
    if to_csv:
        df.to_csv('WSBRedditSentiment.csv')
    else:
        # This is used for retrieving yesterday's sentiment data.
        recent_row = comments[-1]
        date = recent_row[0]
        bull = recent_row[1]
        bear = recent_row[2]
        bull_bear_ratio = bull/bear
        return {
            'Date': date,
            'Bull Comments': bull,
            'Bear Comments': bear,
            'Bull/Bear Ratio': bull_bear_ratio
        }


# Get the sentiment data on WSB for the last 365 days.
#get_sentiment_for_time_period(365, to_csv=True)
