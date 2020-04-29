import string

import matplotlib.pyplot as plt
from nltk import word_tokenize
from nltk.corpus import stopwords  # Words that don't have much meaning (i.e. "I", "a", "the", etc.)
#from nltk.sentiment.vader import SentimentIntensityAnalyzer

#senti = SentimentIntensityAnalyzer()

stop_words = set(stopwords.words("english"))


# We need to:
# 1) Convert everything to lowercase
# 2) Remove punctuation.

# str1: list of characters that need to be replaced.
# str2: specifies the list of characters with which the characters need to be replaced.
# str3: specifies list of characters that need to be deleted

# Returns: returns translation table which specifies conversions that can be used.
def clean_text(text):
    lower_case = text.lower()
    return lower_case.translate(str.maketrans('','', string.punctuation))


# Returns: a list of the words.
def tokenize_words(cleaned_text):
    return word_tokenize(cleaned_text,"english")

# Get the important words, and their frequencies.
def get_final_words(tokens):
    final_words = {}
    for word in tokens:
        if word not in stop_words:
            # Check if we have the word in our dictionary already. Then deal with the frequency.
            if word not in final_words:
                final_words[word] = 1
            else:
                final_words[word] += 1
    return final_words

# Returns a dictionary with scores: {'neg', 'neu', 'pos', 'compound'}
# def sentiment_analyse(cleaned_text):
#     score = senti.polarity_scores(cleaned_text)
#     return score


# Graph the word by frequency using matplotlib.
def graph_sentiment(sentiment_dict):
    plt.bar(sentiment_dict.keys(), sentiment_dict.values())
    plt.savefig('sentiment.png')
    plt.show()




