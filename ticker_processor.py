# Function to Process individual ticker data

import pandas as pd
import numpy as np

# time series processing - df split on ticker
def ticker_processor_minute(df):
    '''
    Preprocess individual ticker data, minute interval data
    
    Calculates the following:
    - simple moving average of the closing price
    - simple moving average of the volume
    - exponential moving average of the closing price
    - standard deviation of the closing price
    - ratio of the standard deviation to price for closing price
    - past performance
    - future performance
    - past performance moving average
    - past performance standard deviation
    - sharpe ratio
    - ATR average true range
    - RSI relative strength index
    - ADX Average Directional Index
    - stochastic oscillator
    - Cross period ratios of various indicators
    
    Parameters
    ----------
    df : pd.DataFrame
        individual ticker data
        
    Returns
    ----------
    df : pd.DataFrame
        ticker data with added columns
    '''
    # intermediate columns used in calcs to delete at the end
    delete_cols_after = []
    
    # resample to minutes, fill in missing values
    df.set_index('timestamp', inplace=True)
    df.index = pd.to_datetime(df.index)
    df = df.resample('1min').asfreq()
    df.fillna(method='ffill', inplace=True)
    
    # lowercase column names
    df.columns = [col.lower() for col in df.columns]
    
    # ----------------------------------------------------------------------
    # calculations that are not period dependent
    # true range - no time period so no loop
    df['true_range'] = (df['high'] - df['low'])
    
    # RSI - relative strength index
    df['diff'] = df.close.diff()
    df['up'] = df['diff']
    df.loc[(df['up']<0), 'up'] = 0
    df['down'] = df['diff']
    df.loc[(df['down']>0), 'down'] = 0
    df['down'] = abs(df['down'])
    delete_cols_after += ['diff', 'up', 'down']
    
    #average directional index (ADX)
    df['prev_high'] = df['high'].shift(1)
    df['prev_low'] = df['low'].shift(1)
    df['DM_pos'] = np.where(~np.isnan(df.prev_high),
        np.where((df['high'] > df['prev_high']) &
        (((df['high'] - df['prev_high']) > (df['prev_low'] - df['low']))),
        df['high'] - df['prev_high'],
        0),np.nan)
    df['DM_neg'] = np.where(~np.isnan(df.prev_low),
        np.where((df['prev_low'] > df['low']) &
        (((df['prev_low'] - df['low']) > (df['high'] - df['prev_high']))),
        df['prev_low'] - df['low'],
        0),np.nan)
    delete_cols_after += ['prev_high', 'prev_low', 'DM_pos', 'DM_neg']
    
    # ----------------------------------------------------------------------
    # loop over all wanted time periods
    for minute in [5, 10, 15, 30, 60, 4*60, 24*60, 7*24*60, 30*24*60]:
        
        # simple moving average
        df['close_sma_{}'.format(minute)] = df.close.rolling(window=minute).mean()
        
        # volume simple moving average
        df['volume_sma_{}'.format(minute)] = df.volume.rolling(window=minute).mean()

        # exponential moving average
        df['close_ema_{}'.format(minute)] = df.close.ewm(halflife=minute).mean()

        # standard deviation of closing price - historical vol
        df['close_stdev_{}'.format(minute)] = df.close.rolling(window=minute).std()

        # close standard deviation / price ratio 
        df['close_stdev_ratio_{}D'.format(minute)] = df['close_stdev_{}'.format(minute)] / df.close

        # forward price performance
        df['close_pct_change_fwd_{}'.format(minute)] = df.close.transform(lambda x: 1 - x/x.shift(-minute)) 
        
        # past price performance
        df['close_pct_change_past_{}'.format(minute)] = df.close.transform(lambda x: 1 - x.shift(minute)/x)
    
        # past performance moving average
        df['close_pct_change_past_ma_{}'.format(minute)] = df['close_pct_change_past_{}'.format(minute)].rolling(window=minute).mean()
    
        # standard deviation of past performance
        df['close_pct_change_past_stdev_{}'.format(minute)] = df['close_pct_change_past_{}'.format(minute)].rolling(window=minute).std()

        # sharpe ratio
        df['sharpe_{}'.format(minute)] = df['close_ma_{}'.format(minute)] / df['close_pct_change_past_stdev_{}'.format(minute)]
        
        # average true range
        df['avg_true_range_{}'.format(minute)] = df['true_range'].rolling(minute).mean()
        
        # RSI - relative strength index
        df['avg_up_{}'.format(minute)] = df['up'].transform(lambda x: x.rolling(window=minute).mean())
        df['avg_down_{}'.format(minute)] = df['down'].transform(lambda x: x.rolling(window=minute).mean())
        df['RS_{}'.format(minute)] = df['avg_up_{}'.format(minute)] / df['avg_down_{}'.format(minute)]
        df['RSI_{}'.format(minute)] = 100 - (100/(1+df['RS_{}'.format(minute)]))
        delete_cols_after += ['avg_up_{}'.format(minute), 'avg_down_{}'.format(minute), 'RS_{}'.format(minute)]

        # average directional index (ADX)
        df['DM_pos_{}'.format(minute)] = df['DM_pos'].ewm(halflife=minute).mean()
        df['DM_neg_{}'.format(minute)] = df['DM_neg'].ewm(halflife=minute).mean()
        df['DI_pos_{}'.format(minute)] = (df['DM_pos_{}'.format(minute)]/df['avg_true_range_{}'.format(minute)])*100
        df['DI_neg_{}'.format(minute)] = (df['DM_neg_{}'.format(minute)]/df['avg_true_range_{}'.format(minute)])*100
        df['DX_{}'.format(minute)] = (np.round(abs(df['DI_pos_{}'.format(minute)] - df['DI_neg_{}'.format(minute)])/(df['DI_pos_{}'.format(minute)] + df['DI_neg_{}'.format(minute)])*100))
        df['ADX_{}'.format(minute)] = df['DX_{}'.format(minute)].ewm(halflife=minute).mean()/100
        delete_cols_after += ['DM_pos_{}'.format(minute), 'DM_neg_{}'.format(minute), 'DI_pos_{}'.format(minute), 'DI_neg_{}'.format(minute), 'DX_{}'.format(minute)]
    
        # stochastic oscillator
        df['lowest_{}'.format(minute)] = df['low'].rolling(window = minute).min()
        df['high_{}'.format(minute)] = df['high'].rolling(window = minute).max()
        df['Stochastic_{}'.format(minute)] = ((df['close'] - df['lowest_{}'.format(minute)])/(df['high_{}'.format(minute)] - df['lowest_{}'.format(minute)]))*100
        df['Stochastic_pct_{}'.format(minute)] = df['Stochastic_{}'.format(minute)].rolling(window = minute).mean()
        delete_cols_after += ['lowest_{}'.format(minute), 'high_{}'.format(minute), 'Stochastic_{}'.format(minute)]

    # ----------------------------------------------------------------------
    # cross period ratios
    for minute1, minute2 in combinations([5, 10, 15, 30, 60, 4*60, 24*60, 7*24*60, 30*24*60], 2):
        # moving averages cross ratios
        df['close_sma_ratio_{}_{}'.format(minute2,minute1)] = df['close_sma_{}'.format(minute2)] / df['close_sma_{}'.format(minute1)]
        df['volume_sma_ratio_{}_{}'.format(minute2,minute1)] = df['volume_sma_{}'.format(minute2)] / df['volume_sma_{}'.format(minute1)]
        df['close_ema_ratio_{}_{}'.format(minute2,minute1)] = df['close_ema_{}'.format(minute2)] / df['close_ema_{}'.format(minute1)]

        # stochastic oscillator
        df['Stochastic_Ratio_{}_{}'.format(minute1, minute2)] = df['Stochastic_pct_{}'.format(minute1)]/df['Stochastic_pct_{}'.format(minute2)]
        
        # RSI - relative strength index
        df['RSI_ratio_{}_{}'.format(minute1, minute2)] = df['RSI_{}'.format(minute1)]/df['RSI_{}'.format(minute2)]
        
    # ----------------------------------------------------------------------
    # drop extra columns
    df = df.drop(columns=delete_cols_after, errors='ignore')
    
    # ----------------------------------------------------------------------
    # return results
    return df        
        

        
def ticker_processor_daily(df):
    '''
    Preprocess individual ticker data, daily interval data
    
    Parameters
    ----------
    df : pd.DataFrame
        individual ticker data
        
    Returns
    ----------
    df : pd.DataFrame
        ticker data with added columns
    '''
    
    # resample to daily
    
    
    

# cross asset, single date - df split on date
