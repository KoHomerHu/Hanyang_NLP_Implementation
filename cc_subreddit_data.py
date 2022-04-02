import requests
from csv import DictReader, DictWriter
import pandas as pd
from datetime import datetime
import praw
import multiprocessing

def get_pushshift_data(data_type, **kwargs):
    base_url = f"https://api.pushshift.io/reddit/search/{data_type}/"
    request = requests.get(base_url, params=kwargs)
    return request.json()

        
def get_data(before, after, batch_id, size = 20):
    reddit = praw.Reddit(
        client_id = '63UKv9Swsihz50RrdRchjA', 
        client_secret = 'jcFfZDuuREplPRZaiJzA3LNv3PjCBg',
        user_agent = 'Homer_Hu')
    data_type = "submission"
    subreddit = "CryptoCurrency"
    aggs = ['url', 'title', 'selftext', 'created_utc']
    filename = 'cc_subreddit_' + str(batch_id) + ".csv"
    pd.DataFrame({'title':[], 'content':[], 'score':[], 'time':[], 'url':[]}).to_csv(filename, index = False)
    with open(filename, 'a', newline = '') as f:
        for start_time in range(before, after, 86400):
            end_time = start_time + 86400
            cc_subreddit_submissions = get_pushshift_data(
                data_type = data_type, 
                after = start_time,
                before = end_time,
                subreddit = subreddit, 
                aggs = aggs,
                size = size
            )
            for post in cc_subreddit_submissions['data']:
                try:
                    post['score'] = reddit.submission(url = post['url']).score
                    # PSAW does not update score, therefore have to obtain score through PRAW
                    DictWriter(f, fieldnames = ['title', 'content', 'score', 'time', 'url']).writerow({
                        'title':post['title'], 
                        'content':post['selftext'], 
                        'score':post['score'], 
                        'time':datetime.fromtimestamp(post['created_utc']),
                        'url':post['url']})
                except:
                    continue
    print(str(batch_id) + "th batch scraping completed.")
                
if __name__ == '__main__':
    
    pool = multiprocessing.Pool(processes = 4)
    
    start_epoch = int(datetime(2021, 3, 11).timestamp())
    end_epoch = int(datetime(2022, 2, 15).timestamp())

    batch_size = 5
    count = 1

    for start_time in range(start_epoch, end_epoch, batch_size*86400):
        end_time = start_time + batch_size * 86400
        pool.apply(get_data, (start_time, end_time, count))
        count += 1

    pool.close()
    pool.join()

    print("Data Collection Complete.")
    
    files = [('cc_subreddit_' + str(i) + ".csv") for i in range(1, count)]
    cc_subreddit = pd.concat([pd.read_csv(file) for file in files ])
    cc_subreddit.to_csv( "cc_subreddit.csv", index = False)

    print("CSV File Generation Complete.")
    
    import os
    
    for file in files:
        os.remove(file)
        
    print("Temporary Data Deletion Complete.")
