
# pull data from bitmex

import pandas as pd
from bitmex import bitmex
import datetime
import pytz
import time
from ratelimit import limits, sleep_and_retry


class BitmexAPI:
    
    def __init__(self, api_key, api_secret):
        self.bclient =  bitmex(test=False, api_key=api_key, api_secret=api_secret)  # init mitmex api client

        
    @sleep_and_retry
    @limits(calls=1, period=1)
    def hit_api(ticker, start_date, end_date, bin_size='1m', count=1000):
        '''
        Rate-limited function to prevent triggering api rate limit
        '''
        return self.bclient.Trade.Trade_betBucketed(symbol=ticker, binSize=bin_size, count=count, startTime=start_date, endTime=end_date).result()[0]
    
    
    def get_data(ticker, start_date='01/01/2015', end_date=datetime.datetime.now().strftime("%m/%d/%Y"), filename=""):
        '''
        Function to pull data from BitMEX API
        '''
        print("starting data pull for {}: {} -> {}".format(symbol, start_date, end_date))
        
        # time bounds of pull
        start_date = datetime.datetime.strptime(start_date, '%m/%d/%Y')
        end_date = datetime.datetime.strptime(end_date, '%m/%d/%Y')
        
        # filename
        if filename == "":
            filename = "bitmex_{}_{}_{}.csv".format(ticker, start_date.strftime("%Y%m%d"), end_date.strftime("%Y%m%d"))
        
        try:
            on_date = start_date        
            while on_date < end_date:

                results = hit_api(ticker, on_date, end_date)

                # check for no data, start date too early
                if len(results) == 0:
                    on_date += datetime.timedelta(weeks=4) # try moving forward 1 month
                else:
                    # get new start date = last date returned
                    on_date = results[len(results)-1]['timestamp'].replace(tzinfo=pytz.utc).replace(tzinfo=pytz.utc)

                    # parse returned data
                    staging.append(item)

            # pd.DataFrame to handle cleaning and output
            data = pd.DataFrame(staging)
            data['timestamp'] = pd.to_datetime(data['timestamp'], unit='ms')
            data = data.drop_duplicates()
            data.set_index('timestamp', inplace=True)
            data.to_csv(filename)

            print("done: {}".format(filename))
            return data, filename

        # catch exceptions
        except Exception as e:
            print(e)
            
            # try to still output data
            data = pd.DataFrame(staging)
            data.to_csv(filename)
            print('finished with error')
            return data, filename
