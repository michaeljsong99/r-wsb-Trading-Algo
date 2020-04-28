# Flask API.

from flask import Flask
import aws

app = Flask(__name__)

@app.route('/5d')
def get_5d_backtest():
    return aws.get_5d_backtest()

@app.route('/1m')
def get_1m_backtest():
    return aws.get_1m_backtest()

@app.route('/6m')
def get_6m_backtest():
    return aws.get_6m_backtest()

@app.route('/1y')
def get_1y_backtest():
    return aws.get_1y_backtest()

@app.route('/ytd')
def get_ytd_backtest():
    return aws.get_ytd_backtest()

@app.route('/max')
def get_max_backtest():
    return aws.get_max_backtest()

@app.route('/5dwsb')
def get_5d_wsb():
    return aws.get_5d_wsb_data()

@app.route('/1mwsb')
def get_1m_wsb():
    return aws.get_1m_wsb_data()

@app.route('/6mwsb')
def get_6m_wsb():
    return aws.get_6m_wsb_data()

@app.route('/1ywsb')
def get_1y_wsb():
    return aws.get_1y_wsb_data()

@app.route('/ytdwsb')
def get_ytd_wsb():
    return aws.get_ytd_wsb_data()

@app.route('/maxwsb')
def get_max_wsb():
    return aws.get_max_wsb_data()

