import pandas_market_calendars as mcal
import yfinance as yf
from datetime import datetime, timedelta
from Reddit import *
import smtplib
import os

password = os.environ['PASSWORD']
print('Password is' + password)


nyse = mcal.get_calendar('NYSE')

# Determine if a given date is a trading date or not.
def is_trading_day(input_date):
    trading_days = nyse.valid_days(start_date=input_date, end_date=input_date)
    if len(trading_days) == 1:
        return True
    return False

# Given a date, determine if tomorrow is a trading day.
def is_tomorrow_trading_day(input_date):
    current_date = datetime.datetime.strptime(input_date, '%Y-%m-%d')
    next_day = current_date + timedelta(days=1)
    next_day_as_str = next_day.strftime("%Y-%m-%d")
    return is_trading_day(next_day_as_str)

def get_all_trading_days(start_date = "2019-04-23", end_date = datetime.datetime.now().strftime('%Y-%m-%d')):
    trading_days = nyse.valid_days(start_date, end_date)
    return trading_days

# Given a date, get tomorrow's date.
def tomorrow_date(input_date):
    current_date = datetime.datetime.strptime(input_date, '%Y-%m-%d')
    next_day = current_date + timedelta(days=1)
    return next_day.strftime("%Y-%m-%d")

# Given an input_date, get yesterday's date.
def yesterday_date(input_date = datetime.datetime.now().strftime("%Y-%m-%d")):
    current_date = datetime.datetime.strptime(input_date, '%Y-%m-%d')
    prev_day = current_date - timedelta(days=1)
    return prev_day.strftime("%Y-%m-%d")

# Given an input date, get the date one month ago.
def one_month_ago(input_date = datetime.datetime.now().strftime("%Y-%m-%d")):
    current_date = datetime.datetime.strptime(input_date, '%Y-%m-%d')
    month_ago = current_date - timedelta(days=int(365/12))
    return month_ago.strftime("%Y-%m-%d")

# Given an input date, get the date six months ago.
def six_month_ago(input_date = datetime.datetime.now().strftime("%Y-%m-%d")):
    current_date = datetime.datetime.strptime(input_date, '%Y-%m-%d')
    six_months_ago = current_date - timedelta(days=int(6 * 365/12))
    return six_months_ago.strftime("%Y-%m-%d")

# Given an input date, get the date one year ago.
def year_ago(input_date = datetime.datetime.now().strftime("%Y-%m-%d")):
    current_date = datetime.datetime.strptime(input_date, '%Y-%m-%d')
    year_ago = current_date - timedelta(days=365)
    return year_ago.strftime("%Y-%m-%d")

# Given an input date, get the date on January 1st of the current year.
def ytd(input_date = datetime.datetime.now().strftime("%Y-%m-%d")):
    current_date = datetime.datetime.strptime(input_date, '%Y-%m-%d')
    start = current_date.replace(month=1, day=1)
    return start.strftime("%Y-%m-%d")

# Gets yesterday's WSB sentiment. # Returns a dictionary with Date, Bull Comments, Bear Comments, Bull/Bear Ratio.
def get_yesterday_wsb_sentiment():
    return get_sentiment_for_time_period(period_length_in_days=1, to_csv=False)

# Calculates the date n entries ago with a WSB_Sentiment Entry (in order to calculate the 10d Bull/Bear EMA).
def get_wsb_date_n_entries_ago(n):
    entries_counted = 0
    date = yesterday_date()
    while entries_counted < n:
        if is_trading_day(date): # Then we know the day before is a date in the WSB sentiment database.
            entries_counted += 1
        date = yesterday_date(input_date=date) # Move back the date.
    return date

# Gets yesterday's SPY Open and closing price. Returns a dictionary with Date, Open, Close.
def get_yesterday_spy_data():
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    yesterday = yesterday_date()
    data = yf.download('SPY', yesterday, current_date)
    row = data.iloc[0]
    date = data.index[0].strftime('%Y-%m-%d')
    open = row['Open']
    close = row['Close']
    return {
        'Date': date,
        'Open': open,
        'Close': close
    }

# Gets all SPY data and returns a list of dictionaries for each date.
def get_all_spy_data():
    start_date = "2019-04-23"
    current_date = datetime.datetime.now().strftime('%Y-%m-%d')
    data = yf.download('SPY', start_date, current_date)
    records = {}
    for index, row in data.iterrows():
        date = index.strftime('%Y-%m-%d')
        open = row['Open']
        close = row['Close']
        records[date] = {
            'Open': open,
            'Close': close
        }
    print(records)
    return records

# Performs a backtest. Assumes we have a $10,000 portfolio to start, and we are constrained by 1x gross leverage.
def backtest(wsb_df, spy_df, portfolio_value = 10000):
    # Idea: If the bull_bear ratio for yesterday is greater than the EMA, then buy at the open.
    # This is a positive sentiment momentum buy signal.
    # If the bull_bear ratio for yesterday is less than the EMA, then sell at the open.
    # This is a negative sentiment momentum sell signal.

    portfolio = portfolio_value
    cash = portfolio_value
    num_shares = 0
    initialized_spy = False
    spy_start_price = None

    portfolio_dict = {}

    for index, row in wsb_df.iterrows():
        next_trading_day = tomorrow_date(index)

        try:
            spy_next_open_price = spy_df.loc[next_trading_day]['Open']
            if initialized_spy is False:
                initialized_spy = True
                spy_start_price = spy_next_open_price
            spy_next_close_price = spy_df.loc[next_trading_day]['Close']
        except:
            continue

        old_num_shares = num_shares
        action = 'No Trade'

        if row['Bull/Bear Ratio'] > row['10d EMA Bull/Bear Ratio']: # Bullish trend.
            if num_shares <= 0: # We can buy shares. Otherwise, keep holding our shares.
                # First close out any existing positions (if we have a net short).
                close_short = num_shares * (-1)
                cash -= (close_short * spy_next_open_price)
                num_shares = 0
                # Use the remaining cash we have to buy shares at the open price.
                num_shares = int(cash // spy_next_open_price)
                cash -= (num_shares * spy_next_open_price)
                shares_bought = num_shares - old_num_shares
                action = 'BUY ' + str(shares_bought) + ' shares at Open'

        elif row['Bull/Bear Ratio'] < row['10d EMA Bull/Bear Ratio']: # Bearish trend.
            if num_shares >= 0: # We can short shares. Otherwise, keep holding our short position.
                cash += (num_shares * spy_next_open_price)
                num_shares = 0
                # Now we can short up to 1x gross leverage.
                num_shares = -(int(cash // spy_next_open_price))
                cash += (-num_shares * spy_next_open_price)
                shares_sold = old_num_shares - num_shares
                action = 'SELL ' + str(shares_sold) + ' shares at Open'

        else:
            pass

        # Now, update our portfolio value.
        holding_value = num_shares * spy_next_close_price
        portfolio = cash + holding_value

        net_profit = portfolio - portfolio_value
        profit_ratio = portfolio / portfolio_value

        # Calculate the spy return.
        spy_return = spy_next_close_price / spy_start_price

        portfolio_dict[next_trading_day] = [portfolio, cash, num_shares, net_profit, profit_ratio, action, spy_return]

    # At the end, we return the dataframe of backtest results.
    backtest_df = pd.DataFrame.from_dict(portfolio_dict, orient='index')
    backtest_df.columns = ['Portfolio Value', 'Net Cash', 'Net Shares', 'Net Profit', 'Backtest Return', 'Action', 'SPY Return']
    return backtest_df

# send emails.
def send_email():
    content = 'WSB Data was successfully updated.'
    mail = smtplib.SMTP('smtp.gmail.com', 587)
    mail.ehlo()
    mail.starttls()
    mail.login('mjysong@gmail.com', password)
    mail.sendmail('mjysong@gmail.com', 'mjysong@gmail.com', content)
    mail.close()