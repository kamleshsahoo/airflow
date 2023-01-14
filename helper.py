from transformers import AutoModelForSequenceClassification
#from transformers import TFAutoModelForSequenceClassification
from transformers import AutoTokenizer, AutoConfig
from scipy.special import softmax
import numpy as np, pandas as pd
import requests, pendulum

class tweet_scrapper:
    def __init__(self, formatted_date, token):
        self.base_url = "https://api.twitter.com/2/tweets/search/recent"
        self.start_date = formatted_date #pendulum.now(tz='UTC').subtract(days=1).format('YYYY-MM-DDTHH:mm:ss') + '.000Z'
        self.query =  "lang%3Aen%20%22S%26P500%22%20-is%3Aretweet" 
        self.max_tweets_pp = 10
        self.token = token
        self.cols_tweets = ['tweet_id', 'auth_id', 'created_at', 'tweet', 'lang', 'retweet_cnt', 'like_cnt']
        self.cols_users = ['auth_id', 'username', 'verified']

    def get_url(self, pag_token=None):
        url_templated = '''{}?query={}&start_time={}&max_results={}'''
        fields = '''&tweet.fields=context_annotations,created_at,lang,public_metrics&expansions=author_id&user.fields=verified'''
        if pag_token!=None:
            url = url_templated+'&pagination_token={}'+fields
            url = url.format(self.base_url, self.query, self.start_date, self.max_tweets_pp, pag_token)
        else:
            url = url_templated+fields
            url = url.format(self.base_url, self.query, self.start_date, self.max_tweets_pp)
        return url

    def response_check(self, response):
        if response.status_code != 200:
            #print(resp.json())
            raise Exception("An error occured: ",response.text)
        else:
            return response.json()


    def scrap(self, all_results=False):
        url = self.get_url()
        resp = requests.get(url=url, headers={'Authorization': 'Bearer {}'.format(self.token)})
        # check status 200 & jsonify it
        resp = self.response_check(resp)
    
        tweets = [[tweet_data['id'], tweet_data['author_id'], pendulum.parse(tweet_data['created_at']).to_datetime_string(), tweet_data['text'], tweet_data['lang'], 
                        tweet_data['public_metrics']['retweet_count'], tweet_data['public_metrics']['like_count']] for tweet_data in resp['data']]
        users = [[user_data['id'], user_data['username'], user_data['verified']] for user_data in resp['includes']['users']]

        if all_results:
            next_token = resp['meta']['next_token']
            more_results = True
            page_cnt = 1
            while more_results:
                page_cnt += 1
                url = self.get_url(pag_token = next_token)
                resp = requests.get(url=url, headers={'Authorization': 'Bearer {}'.format(self.token)})
                # check status 200 & jsonify it
                resp = self.response_check(resp)

                for tweet_data in resp['data']:
                    tweets.append([tweet_data['id'], tweet_data['author_id'], pendulum.parse(tweet_data['created_at']).to_datetime_string(), tweet_data['text'], tweet_data['lang'], 
                        tweet_data['public_metrics']['retweet_count'], tweet_data['public_metrics']['like_count']])
                for user_data in resp['includes']['users']:
                    users.append([user_data['id'], user_data['username'], user_data['verified']])
                
                if "next_token" in resp['meta']:
                    next_token = resp['meta']['next_token']
                else:    
                    more_results = False

            print("total pages retreived: {}".format(page_cnt))

        tweet_df = pd.DataFrame(tweets, columns=self.cols_tweets)
        users_df = pd.DataFrame(users, columns=self.cols_users)

        final_df = tweet_df.merge(users_df, how='left', on='auth_id').drop(columns=['auth_id'])
        return final_df


class sentiment_classifier:
    def __init__(self):
        MODEL = f"cardiffnlp/twitter-roberta-base-sentiment-latest"
        self.tokenizer = AutoTokenizer.from_pretrained(MODEL)
        self.config = AutoConfig.from_pretrained(MODEL)
        # PT
        self.model = AutoModelForSequenceClassification.from_pretrained(MODEL)


    def sentiment(self, tweet):
        new_text = []
        for t in tweet.split(" "):
            t = '@user' if t.startswith('@') and len(t)>1 else t
            t = 'http' if t.startswith('http') else t
            new_text.append(t)
        text = " ".join(new_text)
        #print('tweet: ', tweet)
        #print('cleaned tweet: ', text)
        # text = "Covid cases are increasing fast!"
        # text = preprocess(text)
        encoded_input = self.tokenizer(text, return_tensors='pt')
        output = self.model(**encoded_input)
        scores = output[0][0].detach().numpy()
        scores = softmax(scores)
        # Print labels and scores
        ranking = np.argsort(scores)
        ranking = ranking[::-1]
        score_dict = {}
        for i in range(scores.shape[0]):
            l = self.config.id2label[ranking[i]]
            s = scores[ranking[i]]
            score_dict[l] = np.round(float(s), 4)
            #print(f"{i+1}) {l} {np.round(float(s), 4)}")
        #print('printing labels')
        return score_dict