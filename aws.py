# AWS SDK for Python.
import boto3
from aws_credentials import ACCESS_KEY_ID, SECRET_ACCESS_KEY
from boto3.dynamodb.conditions import Key, Attr
import datetime
import json

import pandas as pd
import utils


# Import decimal since DynamoDB cannot handle floats.
from decimal import Decimal

# Create a AWS session.
session = boto3.Session(
    aws_access_key_id= ACCESS_KEY_ID,
    aws_secret_access_key= SECRET_ACCESS_KEY,
    region_name='us-east-2'
)


DB = session.resource('dynamodb')

spy_table = DB.Table('spy_db')
spy_columns = ['Date', 'Open', 'Close']

wsb_table = DB.Table('wsb_reddit_db')
wsb_columns = ['Date', 'Bull Comments', 'Bear Comments', 'Bull/Bear Ratio', '10d EMA Bull/Bear Ratio']

# Backtest tables.
five_day_backtest = DB.Table('five_day_backtest')
one_m_backtest = DB.Table('one_m_backtest')
six_m_backtest = DB.Table('six_m_backtest')
one_y_backtest = DB.Table('one_y_backtest')
ytd_backtest = DB.Table('ytd_backtest')
max_backtest = DB.Table('max_backtest')

backtest_columns = ['Date', 'Portfolio Value', 'Net Cash', 'Net Shares', 'Net Profit', 'Backtest Return', 'Action', 'SPY Return']

#######################################################################################################
# Adding to AWS database.
#######################################################################################################
# Put a row into the spy historical data table.
def add_to_spy_table(date, open, close):
    response = spy_table.put_item(
        Item= {
            spy_columns[0]: date,
            spy_columns[1]: open,
            spy_columns[2]: close
        })
    return str(response["ResponseMetadata"]["HTTPStatusCode"]) # Should print 200 if it is successful.

# Note, the numbers must be in Decimal form.
def add_to_wsb_table(date, bull_com, bear_com, bull_bear_ratio, ten_d_ema):
    print('Adding entry to wsb_reddit_db table...')
    response = wsb_table.put_item(
        Item= {
            wsb_columns[0]: date,
            wsb_columns[1]: bull_com,
            wsb_columns[2]: bear_com,
            wsb_columns[3]: bull_bear_ratio,
            wsb_columns[4]: ten_d_ema
        }
    )
    print(response["ResponseMetadata"]["HTTPStatusCode"])

def add_to_backtest_table(table, date, portfolio, cash, shares, profit, backtest_return, action, spy_return):
    # Note that the put_item will also replace an existing item with the same date.
    response = table.put_item(
        Item= {
            backtest_columns[0]: date,
            backtest_columns[1]: portfolio,
            backtest_columns[2]: cash,
            backtest_columns[3]: shares,
            backtest_columns[4]: profit,
            backtest_columns[5]: backtest_return,
            backtest_columns[6]: action,
            backtest_columns[7]: spy_return
        }
    )


#######################################################################################################
# Fetching data from AWS database.
#######################################################################################################
# Returns a dataframe of historical WSB Reddit sentiment data. Used for Backtesting.
def get_wsb_data(start_date = '2019-04-23', end_date = datetime.datetime.now().strftime('%Y-%m-%d')):
    print('Getting WSB data from AWS...')
    try:
        fe = Key('Date').between(start_date, end_date)
        pe = "#d, #bb, #10d"
        ean = {"#d": "Date",
               "#bb": "Bull/Bear Ratio",
               "#10d": "10d EMA Bull/Bear Ratio"}
        print(wsb_table)
        response = wsb_table.scan(
            FilterExpression = fe,
            ProjectionExpression = pe,
            ExpressionAttributeNames = ean
        )
        df = pd.DataFrame(response['Items'])
        df.set_index('Date', inplace=True)
        # Sort by most recent date in first row of dataframe.
        df.sort_index(axis=0, inplace=True)
        print('Retrieved WSB data successfully.')
        return df
    except:
        raise Exception('There was an error with retrieving WSB data from AWS.')

# Gets all the WSB data from Reddit.
def get_all_wsb_data():
    date = datetime.datetime(2019, 4, 24)
    end_date = datetime.datetime.now()
    current_wsb_data = get_wsb_data()
    dates_to_add_data = []
    trading_days = utils.get_all_trading_days().values
    trading_day_set = set()
    for td in trading_days:
        trading_day_set.add(str(td)[:10])
    while date <= end_date:
        cur_date = date.strftime('%Y-%m-%d')
        prev_date = (date - datetime.timedelta(days=1)).strftime('%Y-%m-%d')
        if cur_date in trading_day_set:
            # We need to make sure we have the wsb data for this date.
            if prev_date not in current_wsb_data.index:
                dates_to_add_data.append(prev_date)
        date += datetime.timedelta(days=1)
    print('There is missing WSB data on ' + str(len(dates_to_add_data)) + ' dates, shown below.')
    print(dates_to_add_data)
    documents = {}
    for date_str in dates_to_add_data:
        documents[date_str] = utils.get_sentiment_for_day(date_str)
    # Now, we need to calculate the 10d EMA for all of the missing data points.
    bull_bear_ratios = current_wsb_data['Bull/Bear Ratio']
    dates = []
    values = []
    for k, v in documents.items():
        dates.append(k)
        values.append(v['Bull/Bear Ratio'])
    data_to_add = pd.Series(data=values, index=dates)
    bull_bear_ratios = bull_bear_ratios.append(data_to_add)
    bull_bear_ratios.sort_index(inplace=True)
    ema_10 = pd.Series.ewm(bull_bear_ratios, span=10).mean()
    for date, v in documents.items():
        ten_d_ema = Decimal(str(ema_10.get(key = date)))
        bull_com = Decimal(str(v['Bull Comments']))
        bear_com = Decimal(str(v['Bear Comments']))
        bull_bear_ratio = Decimal(str(v['Bull/Bear Ratio']))
        add_to_wsb_table(date, bull_com, bear_com, bull_bear_ratio, ten_d_ema)
    print('Finished updating WSB data.')









# Returns a dataframe of historical SPY data. Used for backtesting.
def get_spy_data(start_date = '2019-04-23', end_date = datetime.datetime.now().strftime('%Y-%m-%d')):
    print('Getting SPY data from AWS...')
    try:
        fe = Key('Date').between(start_date, end_date)
        pe = "#d, #op, #cl"
        ean = {"#d": "Date",
               "#op": "Open",
               "#cl": "Close"}
        response = spy_table.scan(
            FilterExpression = fe,
            ProjectionExpression = pe,
            ExpressionAttributeNames = ean
        )
        df = pd.DataFrame(response['Items'])
        df.set_index('Date', inplace=True)
        # Sort by most recent date in first row of dataframe.
        df.sort_index(axis=0, inplace=True)
        print('Retrieved SPY data successfully.')
        return df
    except:
        raise Exception('There was an error with retrieving SPY data from AWS.')

def get_all_spy_data():
    all_spy_data = utils.get_all_spy_data()
    spy_data_in_db = get_spy_data()
    dates_updated = []
    for date, v in all_spy_data.items():
        if date not in spy_data_in_db.index:
            dates_updated.append(date)
            open = Decimal(str(v['Open']))
            close = Decimal(str(v['Close']))
            add_to_spy_table(date, open, close)
    print('Added SPY data on ' + str(len(dates_updated)) + ' dates, shown below:')
    print(dates_updated)

# Returns all the dates present in a backtest table.
def get_backtest_dates(table):
    print('Getting backtest data from AWS...')
    try:
        pe = "#d"
        ean = {"#d": "Date"}
        response = table.scan(
            ProjectionExpression= pe,
            ExpressionAttributeNames=ean
        )
        dates = [x['Date'] for x in response['Items']]
        return set(dates)
    except:
        raise Exception('There was an error with retrieving backtest data from AWS.')

# Returns a dataframe of backtest results.
def get_backtest_data(table):
    print('Getting backtest data from AWS...')
    try:
        pe = "#d, #bt, #spy"
        ean = {"#d": "Date",
               "#bt": "Backtest Return",
               "#spy": "SPY Return"}
        response = table.scan(
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
        )
        df = pd.DataFrame(response['Items'])
        df.set_index('Date', inplace=True)
        # Sort by most recent date in first row of dataframe.
        df.sort_index(axis=0, inplace=True)
        print('Retrieved backtest data successfully.')
        return df
    except:
        raise Exception('There was an error with retrieving backtest data from AWS.')

# Returns a dataframe of trade history.
def get_history():
    print('Getting backtest data from AWS...')
    try:
        pe = "#d, #pv, #ns, #nc, #a, #np"
        ean = {"#d": "Date",
               "#pv": "Portfolio Value",
               "#ns": "Net Shares",
               "#nc": "Net Cash",
               "#a": "Action",
               "#np": "Net Profit"}
        response = max_backtest.scan(
            ProjectionExpression=pe,
            ExpressionAttributeNames=ean
        )
        df = pd.DataFrame(response['Items'])
        df.set_index('Date', inplace=True)
        # Sort by most recent date in first row of dataframe.
        df.sort_index(axis=0, inplace=True)
        print('Retrieved historical data successfully.')
        return df
    except:
        raise Exception('There was an error with retrieving historical data from AWS.')




#######################################################################################################
# Updating the tables in the AWS database.
#######################################################################################################
# On 3am on the morning of trading days, calculate the WSB sentiment from yesterday.
def update_wsb_table():
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    if utils.is_trading_day(current_date) is False:
        print('Today is not a trading day. WSB Sentiment from yesterday not updated.')
        return

    date_nine_wsb_entries_ago = utils.get_wsb_date_n_entries_ago(9)
    print('Nine entries ago was on: ' + date_nine_wsb_entries_ago)
    #wsb_data = get_wsb_data(start_date=date_nine_wsb_entries_ago)
    wsb_data = get_wsb_data()
    print('Retrieved Historical WSB data.')
    print('Analyzing Sentiment figures...')
    sentiment_dict = utils.get_yesterday_wsb_sentiment()
    print(sentiment_dict)
    print('Retrieved Sentiment from yesterday.')

    # Now calculate the recent 10d moving average.
    bull_bear_series = wsb_data['Bull/Bear Ratio']
    bull_bear_series = bull_bear_series.append(pd.Series([sentiment_dict['Bull/Bear Ratio']], index=[sentiment_dict['Date']]))
    print(bull_bear_series)
    ema_10 = pd.Series.ewm(bull_bear_series, span=10).mean()
    ema_10_value = Decimal(str(ema_10.get(key = sentiment_dict['Date'])))

    # Now, add the entry to the AWS database.
    date = sentiment_dict['Date']
    bull_comments = Decimal(str(sentiment_dict['Bull Comments']))
    bear_comments = Decimal(str(sentiment_dict['Bear Comments']))
    bull_bear_ratio = Decimal(str(sentiment_dict['Bull/Bear Ratio']))

    add_to_wsb_table(date, bull_comments, bear_comments, bull_bear_ratio, ema_10_value)

# At 3am each day, if yesterday was a trading day, then call this method to update the spy database.
def update_spy_table():
    yesterday = utils.yesterday_date()
    if utils.is_trading_day(yesterday) is False:
        print('Yesterday was not a trading day. No SPY data to update.')
        return

    print('Getting SPY Data from yesterday...')
    info = utils.get_yesterday_spy_data()
    print(info)
    print('Retrieved SPY data from yesterday.')
    date = info['Date']
    open = Decimal(str(info['Open']))
    close = Decimal(str(info['Close']))
    add_to_spy_table(date, open, close)

# Calls the update on the SPY and WSB historical tables. Note these may not update based on the time.
def update_tables():
    is_wsb_update = False
    is_spy_update = False
    try:
        print('Calling Update on WSB Table.')
        update_wsb_table()
        is_wsb_update = True
    except:
        raise Exception('WSB_Sentiment database update failed.')

    try:
        print('Calling Update on SPY Table.')
        update_spy_table()
        is_spy_update = True
    except:
        raise Exception('SPY database update failed.')

    # Now update our backtests if there was any change.
    if is_wsb_update or is_spy_update:
        update_backtests()

    print('All updates were successful.')

# In the past, there have been cases where PushShift was not able to get reddit data for a few days.
# In this case, we call the run_full_updates method instead to fill in all the missing days.
def run_full_updates():
    is_wsb_update = False
    is_spy_update = False
    print('Running full data uodate...')
    try:
        print('Updating SPY data...')
        get_all_spy_data()
        is_spy_update = True
    except:
        raise Exception('SPY database update failed.')
    try:
        print('Updating WSB data...')
        get_all_wsb_data()
        is_wsb_update = True
    except:
        raise Exception('WSB_Sentiment database update failed.')

    # Now update our backtests if there was any change.
    if is_wsb_update or is_spy_update:
        update_backtests()
    print('All updates were successful.')






#######################################################################################################
# Performing Backtests.
#######################################################################################################
# Given a start date, perform a backtest of the trading strategy.
# Returns a Dataframe with the index of the Date, columns [Portfolio Value, Net Cash, Net Shares, Net Profit, Backtest Return, Action,
#                                                                       SPY Return]
def backtest(start_date = '2019-04-23'):
    spy_data = get_spy_data(start_date=start_date)
    wsb_data = get_wsb_data(start_date=start_date)
    return utils.backtest(wsb_df=wsb_data, spy_df=spy_data)

# Adds the results of a backtest to the appropriate table in the database.
def add_backtest_to_db(backtest_df, table):
    dates = get_backtest_dates(table)
    for date in dates:
        if date not in backtest_df.index:
            # Delete the date entry from the backtest table.
            response = table.delete_item(
                Key= {
                    'Date': date
                }
            )
            if str(response["ResponseMetadata"]["HTTPStatusCode"]) == '200':
                print('Old record at ' + date + ' deleted.')

    # Now, for each of the rows in my dataframe, add it to the table.
    for index, row in backtest_df.iterrows():
        portfolio = Decimal(str(row['Portfolio Value']))
        cash = Decimal(str(row['Net Cash']))
        shares = Decimal(str(row['Net Shares']))
        profit = Decimal(str(row['Net Profit']))
        backtest_return = Decimal(str(row['Backtest Return']))
        action = row['Action']
        spy_return = Decimal(str(row['SPY Return']))
        # Add to our table.
        add_to_backtest_table(table, index, portfolio, cash, shares, profit, backtest_return, action, spy_return)

# Updates the backtest tables in our AWS database.
def run_5d_backtest():
    print('Running backtest for 5d time period...')
    try:
        five_days_ago = utils.get_wsb_date_n_entries_ago(5)
        backtest_df = backtest(start_date=five_days_ago)
        add_backtest_to_db(backtest_df, table=five_day_backtest)
        print('Backtest and table update completed successfully.')
    except:
        raise Exception('Backtest or Table Update for 5d period failed.')

def run_1m_backtest():
    print('Running backtest for 1 month time period...')
    try:
        one_month_ago = utils.one_month_ago()
        backtest_df = backtest(start_date=one_month_ago)
        add_backtest_to_db(backtest_df, table=one_m_backtest)
        print('Backtest and table update completed successfully.')
    except:
        raise Exception('Backtest or Table Update for 1 month period failed.')

def run_6m_backtest():
    print('Running backtest for 6 month time period...')
    try:
        six_months_ago = utils.six_month_ago()
        backtest_df = backtest(start_date=six_months_ago)
        add_backtest_to_db(backtest_df, table=six_m_backtest)
        print('Backtest and table update completed successfully.')
    except:
        raise Exception('Backtest or Table Update for 6 month period failed.')

def run_ytd_backtest():
    print('Running backtest for YTD time period...')
    try:
        ytd = utils.ytd()
        backtest_df = backtest(start_date=ytd)
        add_backtest_to_db(backtest_df, table=ytd_backtest)
        print('Backtest and table update completed successfully.')
    except:
        raise Exception('Backtest or Table Update for YTD period failed.')

def run_1y_backtest():
    print('Running backtest for 1 year time period...')
    try:
        one_year_ago = utils.year_ago()
        backtest_df = backtest(start_date=one_year_ago)
        add_backtest_to_db(backtest_df, table=one_y_backtest)
        print('Backtest and table update completed successfully.')
    except:
        raise Exception('Backtest or Table Update for 1 year period failed.')

def run_max_backtest():
    print('Running backtest for time period since strategy inception...')
    try:
        backtest_df = backtest()
        add_backtest_to_db(backtest_df, table=max_backtest)
        print('Backtest and table update completed successfully.')
    except:
        raise Exception('Backtest or Table Update for period since inception failed.')

# Called in the update every day.
def update_backtests():
    run_5d_backtest()
    run_1m_backtest()
    run_6m_backtest()
    run_ytd_backtest()
    run_1y_backtest()
    run_max_backtest()
    print('All backtests completed and tables updated.')



########################################################################################################
# JSONifying DataFrames
########################################################################################################
def get_5d_backtest():
    df = get_backtest_data(five_day_backtest)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_1m_backtest():
    df = get_backtest_data(one_m_backtest)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_6m_backtest():
    df = get_backtest_data(six_m_backtest)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_ytd_backtest():
    df = get_backtest_data(ytd_backtest)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_1y_backtest():
    df = get_backtest_data(one_y_backtest)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_max_backtest():
    df = get_backtest_data(max_backtest)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_5d_wsb_data():
    start_date = utils.one_month_ago()
    df = get_wsb_data(start_date=start_date)
    df = df.tail(5)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_1m_wsb_data():
    start_date = utils.one_month_ago()
    df = get_wsb_data(start_date=start_date)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_6m_wsb_data():
    start_date = utils.six_month_ago()
    df = get_wsb_data(start_date=start_date)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_ytd_wsb_data():
    start_date = utils.ytd()
    df = get_wsb_data(start_date=start_date)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_1y_wsb_data():
    start_date = utils.year_ago()
    df = get_wsb_data(start_date=start_date)
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_max_wsb_data():
    df = get_wsb_data()
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

def get_history_data():
    df = get_history()
    return json.dumps(json.loads(df.to_json(orient='index')), indent=2)

########################################################################################################
# Uploading Historical CSV Data
#######################################################################################################
# Upload the data we already have stored in csvs.
def add_historical_wsb():
    wsb = pd.read_csv('wsb_sentiment.csv')
    for index, row in wsb.iterrows():
        date = row['Date']
        bull_com = Decimal(str(row['Comments_Bull']))
        bear_com = Decimal(str(row['Comments_Bear']))
        bull_bear_ratio = Decimal(str(row['Bull_Bear_Ratio']))
        ten_d_ema = Decimal(str(row['EMA_10']))

        add_to_wsb_table(date, bull_com, bear_com, bull_bear_ratio, ten_d_ema)


def add_historical_spy():
    spy_historical = pd.read_csv('SPY.csv')
    for index, row in spy_historical.iterrows():
        date = row['Date']
        open = Decimal(str(row['Open']))
        close = Decimal(str(row['Close']))

        add_to_spy_table(date, open, close)
