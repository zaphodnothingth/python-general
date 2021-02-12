# API/scrape
import requests
user_agent_list = [
   #Chrome
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 5.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.2; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.90 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.3; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/60.0.3112.113 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.87 Safari/537.36',
    #Firefox
    'Mozilla/4.0 (compatible; MSIE 9.0; Windows NT 6.1)',
    'Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; WOW64; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 6.2; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.0; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.3; WOW64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)',
    'Mozilla/5.0 (Windows NT 6.1; Win64; x64; Trident/7.0; rv:11.0) like Gecko',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; WOW64; Trident/6.0)',
    'Mozilla/5.0 (compatible; MSIE 10.0; Windows NT 6.1; Trident/6.0)',
    'Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 5.1; Trident/4.0; .NET CLR 2.0.50727; .NET CLR 3.0.4506.2152; .NET CLR 3.5.30729)'
]

# general python 
import time
import sys
import re
import json
import pandas as pd
from datetime import datetime, timedelta
import csv
import random
# from unidecode import unidecode
import traceback
from requests.exceptions import Timeout, ConnectionError

# multithreading
import threading
import queue
global lck 
lck = threading.Lock()

'''# nlp
import spacy
# python -m spacy download en_core_web_sm
nlp = spacy.load("en_core_web_sm")'''

# start forreal


def url_proc(url):
    try:
        try:
            # randomize user agents
            user_agent = random.choice(user_agent_list)
            headers = {'User-Agent': user_agent}
            response = requests.get("https://" + url,
                            timeout=5,
                            headers=headers)
                        
        except Timeout:
            response = None
        if not response or response.status_code != 200:
            # randomize user agents
            user_agent = random.choice(user_agent_list)
            headers = {'User-Agent': user_agent}
            response = requests.get("http://" + url,
                            timeout=5,
                            headers=headers)
                            
        return str(response.status_code)
    except (ConnectionError, Timeout):
        return 'failure'



class Worker(threading.Thread):


    def __init__(self, q, i, *args, **kwargs):
        self.q = q
        self.i = i
        super().__init__(*args, **kwargs)
        
        
    def run(self):
        while True:
            try:
                j, receive_dict, csv_columns = self.q.get(timeout=3)  # 3s timeout
                i = self.i
            except queue.Empty:
                return
            
            url = receive_dict['URL']
            return_dict = {}
            return_dict['url'] = url
            
            try:
                return_dict['response'] = url_proc(url)
                
            except:
                traceback.print_exc()
                print('[t{}] {}- error: {}\n\tlink: {}\n\n\n\n\n\n\n'.format(i, j, sys.exc_info()[0], url))  
                return_dict['response'] = 'unreachable'   
            
                
            lck.acquire()
            with open("output.csv", 'a',encoding='utf-8-sig', newline='') as g:
                csv.DictWriter(g, fieldnames=csv_columns).writerow(return_dict)
            lck.release()
            print('[t{}] {}- written\n\tlink: {}'.format(i, j, url))
            self.q.task_done()


def main(args):
    print('start time: {}'.format(datetime.now().strftime("%Y-%m-%d-%H.%M.%S")))
    start_time = time.time()

    # n_threads= int(args[0])
    n_threads = 30
    
    target_df = pd.read_csv('inputs.csv') 
    # target_df = target_df[:3] # limit # rows for testing for testing
    print('URL count: {}'.format(len(target_df)))

    ################### add desired output columns
    csv_columns = ['url', 'response']
    with open("output.csv", 'w',encoding='utf-8-sig', newline='') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
    
    q = queue.Queue()
    for k, row in enumerate(target_df.itertuples()):
        dictRow = row._asdict()
        q.put_nowait((k, dictRow, csv_columns))
    for _ in range(n_threads):
        Worker(q, _).start()
        time.sleep(1)
    q.join()
    
    print('finished. end time: {}'.format(datetime.now().strftime("%Y-%m-%d-%H.%M.%S")))
    print('completed in {}'.format(timedelta(seconds=int(time.time() - start_time))))


if __name__ == '__main__':
    main(sys.argv[1:])
