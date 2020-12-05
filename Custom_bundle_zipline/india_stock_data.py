import pandas as pd
from os import listdir
from trading_calendars import get_calendar

# Change the path to where you have your data
path = 'C:\\Users\sabirj\\Desktop\\P4Finance\\bse500 bundle\\bse500'

# path = 'C:\\Users\\sabirj\\Desktop\\P4Finance\\machine-learning-for-trading-master\\machine-learning-for-trading-master\\data\\bse_data'

# C:\Users\sabirj\Desktop\P4Finance\machine-learning-for-trading-master\machine-learning-for-trading-master\data\bse_data


"""
The ingest function needs to have this exact signature,
meaning these arguments passed, as shown below.
"""
def bse_data(environ,
                  asset_db_writer,
                  minute_bar_writer,
                  daily_bar_writer,
                  adjustment_writer,
                  calendar,
                  start_session,
                  end_session,
                  cache,
                  show_progress,
                  output_dir):
    
    # Get list of files from path
    # Slicing off the last part
    # 'example.csv'[:-4] = 'example'
    symbols = [f[:-4] for f in listdir(path)]
    
    if not symbols:
        raise ValueError("No symbols found in folder.")
        
        
    # Prepare an empty DataFrame for dividends
    divs = pd.DataFrame(columns=['sid', 
                                 'amount',
                                 'ex_date', 
                                 'record_date',
                                 'declared_date', 
                                 'pay_date']
    )
    
    # Prepare an empty DataFrame for splits
    splits = pd.DataFrame(columns=['sid',
                                   'ratio',
                                   'effective_date']
    )
    
    # Prepare an empty DataFrame for metadata
    metadata = pd.DataFrame(columns=('start_date',
                                              'end_date',
                                              'auto_close_date',
                                              'symbol',
                                              'exchange'
                                              )
                                     )


    # Check valid trading dates, according to the selected exchange calendar
    sessions = calendar.sessions_in_range(start_session, end_session)
    #sessions = calendar.sessions_in_range('1995-05-02', '2020-05-27')

    
    # Get data for all stocks and write to Zipline
    daily_bar_writer.write(
            process_stocks(symbols, sessions, metadata, divs)
            )

    # Write the metadata
    asset_db_writer.write(equities=metadata)
    
    # Write splits and dividends
    adjustment_writer.write(splits=splits,
                            dividends=divs)    
    
    
"""
Generator function to iterate stocks,
build historical data, metadata 
and dividend data
"""
def process_stocks(symbols, sessions, metadata, divs):
    # Loop the stocks, setting a unique Security ID (SID)
    my_cal = get_calendar('XBOM')
    for sid, symbol in enumerate(symbols):
        
        print('Loading {}...'.format(symbol))
        # Read the stock data from csv file.
        df = pd.read_csv('{}/{}.csv'.format(path, symbol), index_col=[0], parse_dates=[0]) 
        
        # Check first and last date.
        # start_date = df.index[0]
        # end_date = df.index[-1] 

        
        # Valid days
        # valid_days = my_cal.sessions_in_range(start_date, end_date)

        # deleted_rows = df[df.date.isin(valid_days) == False]
        df = df[df.index.isin(sessions)]
        
        start_date = df.index[0]
        end_date = df.index[-1] 

        
        # Synch to the official exchange calendar
        #df = df.reindex(sessions.tz_localize(None))[start_date:end_date] #tz_localize(None)
        df = df.reindex(sessions.tz_localize(None))[start_date:end_date] #tz_localize(None)
        
        # Forward fill missing data
        df.fillna(method='ffill', inplace=True)
        
        # Back fill missing data
        # df.fillna(method='bfill', inplace=True)
        
        # Drop remaining NaN
        df.dropna(inplace=True)    
        
        # The auto_close date is the day after the last trade.
        ac_date = end_date + pd.Timedelta(days=1)
        
        # Add a row to the metadata DataFrame. Don't forget to add an exchange field.
        metadata.loc[sid] = start_date, end_date, ac_date, symbol, 'XBOM'
        
        # If there's dividend data, add that to the dividend DataFrame
        if 'dividend' in df.columns:
            
            # Slice off the days with dividends
            tmp = df[df['dividend'] != 0.0]['dividend']
            div = pd.DataFrame(data=tmp.index.tolist(), columns=['ex_date'])
            
            # Provide empty columns as we don't have this data for now
            div['record_date'] = pd.NaT
            div['declared_date'] = pd.NaT
            div['pay_date'] = pd.NaT            
            
            # Store the dividends and set the Security ID
            div['amount'] = tmp.tolist()
            div['sid'] = sid
            
            # Start numbering at where we left off last time
            ind = pd.Index(range(divs.shape[0], divs.shape[0] + div.shape[0]))
            div.set_index(ind, inplace=True)
            
            # Append this stock's dividends to the list of all dividends
            divs = divs.append(div)    
            
        yield sid, df