# this function is used to calculate single period, multi ticker values

import pandas as pd
import numpy as np


def period_processor(df):
    '''
    Rank period data, on pass in single periods
    
    Parameters
    ----------
    df : pd.DataFrame
        individual period data
        
    Returns
    ----------
    df : pd.DataFrame
        ranked period data
    '''
    # lowercase column names
    df.columns = [col.lower() for col in df.columns]
    
    # columns to exclude from ranking
    no_rank_cols = ['timestamp', 'symbol', 'open', 'high', 'low', 'close', 'homeNotional', 'foreignNotional']
    
    # columns to rank
    columns_to_rank = [col for col in df.columns if col not in no_rank_cols]
    
    # return cols
    return_cols = ['timestamp','symbol']
    
    # rank cols
    for col in rank_cols_full:
        df['{}_rank'.format(col)] = df[col].rank(pct=True)
        return_cols.append('{}_rank'.format(col))
        
    return df[return_cols]
