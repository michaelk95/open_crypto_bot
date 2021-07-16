# data update
import multiprocessing as mp
from sqlalchemy import create_engine
from random import shuffle

from api import BitmexAPI
from preprocessor import ticker_processor_minute, period_processor
import config 


def get_sql_engine():
    # mysql database connection
    schema_name = 'crypto'
    db_address = '127.0.0.1:3306'  # local mySQL instance
    connect_string = 'mysql+pymysql://' + config.MYSQL_USER +':' + config.MYSQL_KEY + '@' + db_address +'/' + schema_name
    return create_engine(connect_string)
    

if __name__ == "__main__":
    
    # get most recent
    engine = get_sql_engine().connect()
    max_dates = pd.read_sql("SELECT ticker, max(date) FROM bitmex GROUP BY ticker", con=engine)
    
    # get data from API
    api_driver = BitmexAPI(config.BITMEX_ID, config.BITMEX_KEY)
    new_data = list()
    for index, row in df.iterrows():
        ticker = row[0]
        start_date = row[1]
        
        api_result, _ = api_driver.get_data(ticker, start_date=start_date)
        
        new_data.append(api_result)
        
    new_data = pd.concat(new_data, ignore_index=True).reset_index()
    
    # push raw data to sql
    new_data.to_sql("bitmex", if_exists='append', con=engine)
    
    # --------------------------------------------------------------------------
    # preprocess data
    three_years_ago = datetime.datetime.now() - datetime.timedelta(years=3)
    df = pd.read_sql("SELECT * FROM bitmex WHERE timestamp > {}".format(three_year_ago.strftime("%Y-%m-%d")), con=engine)
    
    # process and join 
    df_preprocessed = pd.concat([
        mp_processor(ticker_processor_minute, "symbol"),
        mp_processor(period_processor, "timestamp")]
        axis=1)
 
    # push preprocessed data
    df_preprocessed.to_sql("bitmex_preprocessed", if_exists='append', con=engine)
    
    # save local copies
    df_preprocessed.to_csv("bitmex_preprocessed_{}.csv".format(datetime.datetime.now().strftime("%Y%m%d%H%M%S")))
    
    # done
