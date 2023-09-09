#!pip install pathlib
#!pip install praw==7.5.0
import pandas as pd
import os
from pathlib import Path
import numpy as np
import nltk.corpus
import spacy
import re
import string
from profanity_filter import ProfanityFilter
nlp = spacy.load('en_core_web_sm')
import warnings
warnings.filterwarnings("ignore")
import praw
from praw.models import MoreComments
pd.options.mode.chained_assignment = None

subreddit = input('subreddit name ex(cats)')
time = 'all'
# filepath = Path(f"data/raw_data/{subreddit}.csv"
filepath = Path(f"data/raw_data/{subreddit}.csv")  # Location of output CSV
num_posts = 1000  # number of posts parsed from "hot" catagory *note: some will be filtered out, expect about 20% returns*
num_comments = 10  # min number of comments per post
get_comments = 300  # number of comments you want to download

client_id = "RBC5PY5SqCs6PfBrGx6-wg"  # "Script" Public id from https://www.reddit.com/prefs/apps
client_secret = "Emg7uUxiKPSM-lTN822UyGhgY2Ptfg"  # "Script" Private id from https://www.reddit.com/prefs/apps
username = "DataCollector123"  # reddit account username
password = "NotaRobot"  # reddit account password
user_agent = "prawdatacollector"

reddit = praw.Reddit(client_id=client_id,
                     client_secret=client_secret,
                     username=username,
                     password=password,
                     user_agent=user_agent)

subreddit = reddit.subreddit(subreddit)
# subreddit_posts = subreddit.hot(limit=num_posts)
subreddit_posts = subreddit.top(time_filter=time, limit=num_posts)

data = []
post_num = 1
for post in subreddit_posts:
    temp = []
    try:
        comments = post.comments.list()
    except:
        continue
    if len(comments) >= num_comments and post.domain == 'i.redd.it':
        print(post_num)
        post_num += 1
        temp.append(post.title)
        temp.append(post.url)
        temp.append(subreddit)
        # print(post.title)
        j = 0
        while j != get_comments:
            # print(comments[j].body)
            try:
                temp.append(post.comments[j].body)
                j += 1
            except:
                temp.append("fill")
                j += 1
        data.append(temp)
    else:
        post_num += 1

df = pd.DataFrame(data)
result = df.iloc[0:, :]
for i in range(0, len(result.columns) + 1):
    if i == 0:
        result.rename(columns={i: 'title'}, inplace=True)
    if i == 1:
        result.rename(columns={i: 'image_link'}, inplace=True)
    if i == 2:
        result.rename(columns={i: 'subreddit'}, inplace=True)
    else:
        result.rename(columns={i: f'comment{i - 1}'}, inplace=True)

filepath.parent.mkdir(parents=True, exist_ok=True)
result.to_csv(filepath)
print("done")


class Cleaner:
    def __init__(self):
        self.delete_list = ['[deleted]', '[removed]', 'removed', 'deleted', 'https', 'Repost', 'repost', '!gif',
                            '&#x20B', '[OC]', '*',
                            'instagram', 'facebook', 'twitter', 'google', '/r', '/u', 'http', 'www', '/s', '#']
        self.sub_list = [r'r/\S+', r'\([^)]*\)', r'http\S+', r'[()]', r'[:]', r'[\([{})\]]']
        self.pf = ProfanityFilter()
        self.cols = ['title', 'image_link', 'subreddit']

    # Removes emoticons
    def remove_emojis(self, data):
        emoj = re.compile("["
                          u"\U0001F600-\U0001F64F"  # emoticons
                          u"\U0001F300-\U0001F5FF"  # symbols & pictographs
                          u"\U0001F680-\U0001F6FF"  # transport & map symbols
                          u"\U0001F1E0-\U0001F1FF"  # flags (iOS)
                          u"\U00002500-\U00002BEF"  # chinese char
                          u"\U00002702-\U000027B0"
                          u"\U00002702-\U000027B0"
                          u"\U000024C2-\U0001F251"
                          u"\U0001f926-\U0001f937"
                          u"\U00010000-\U0010ffff"
                          u"\u2640-\u2642"
                          u"\u2600-\u2B55"
                          u"\u200d"
                          u"\u23cf"
                          u"\u23e9"
                          u"\u231a"
                          u"\ufe0f"  # dingbats
                          u"\u3030"
                          "]+", re.UNICODE)
        return re.sub(emoj, '', data)

    # removes characters that appear more than twice ex: booo -> boo
    def remove_dupes(self, s):
        ans = ""
        seen = ''
        i = 0
        keep_list = ['s', 'e', 't', 'f', 'l', 'm', 'o', 'p', 'd', 'n', 'g', 'r', 'b']
        while i != (len(s) - 1):
            if s[i] in keep_list and s[i] != seen:
                if s[i] == s[i + 1]:
                    seen = s[i]
                    ans += s[i]
                    ans += s[i + 1]
            if s[i] != seen and s[i + 1] != s[i]:
                seen = s[i]
                ans += s[i]
                i += 1
            else:
                i += 1
        ans += s[len(s) - 1]
        if ans[-1] == ans[-2] and ans[-1] == ans[-2]:
            ans = ans[0:-1]
        return ans

    def splitter(self, text, span):
        s = span
        words = re.sub(r'[^\w\s]', '', text).split(' ')
        t = [" ".join(words[i:i + s]) for i in range(0, len(words), s)]
        return t

    # Subsets dataset and define filters
    def clean(self, DATA, subreddit):
        data = DATA
        data_subset = data.drop(self.cols, axis=1)
        data_first = data[self.cols]
        for i in range(len(data_subset)):
            # for i in range(1,4):
            for j in range(len(data_subset.iloc[i])):
                text = data_subset.iloc[i][j]
                try:
                    text = self.remove_emojis(text)
                except:
                    continue

                try:
                    text = self.remove_dupes(text)
                except:
                    print('dupe removal failed')
                    continue
                text = text.replace("\r", " ").replace("\n", " ").replace("\t", " ").replace("_x000D_", " ").strip()
                try:
                    text = re.sub(r'\([^)]*\)', '', text)
                    for sub in self.sub_list:
                        text = re.sub(sub, '', text)
                except:
                    continue
                # text = self.pf.censor(text)
                if any(word in data_subset.iloc[i][j].lower() for word in self.delete_list):
                    text = 0
                data_subset['comment' + str(j + 2)][i] = text
            print(i, "out of", len(data_subset))

        # format data
        result = pd.concat([data_first, data_subset], axis=1, join='inner')
        result = result.iloc[1:, :]
        df = pd.DataFrame(columns=['SubmissionID', 'SubmissionTitle', 'CommentID', 'Comment', 'subreddit', 'Images'])
        for i in range(len(result)):
            commentid = 1
            title = result.iloc[i][0]
            sub = result.iloc[i][2]
            # sub = subreddit
            image = result.iloc[i][1]
            for j in result.iloc[i][4:]:
                if j == 'fill':
                    continue
                elif j != 0 and len(j) > 0:
                    insert = ['placeholderID', title, commentid, j, sub, image]
                    df = df.append(pd.DataFrame([insert],
                                                columns=['SubmissionID', 'SubmissionTitle', 'CommentID', 'Comment',
                                                         'subreddit', 'Images']))
                    commentid += 1
        return df


obj = Cleaner()
data = result
res = obj.clean(data, subreddit)
res.to_csv(f'data/cleaned_data/{subreddit}.csv', index=False, encoding='utf8')
print('done')