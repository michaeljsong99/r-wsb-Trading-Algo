# Flask API.

from flask import Flask
import aws
from flask_cors import CORS, cross_origin
from apscheduler.schedulers.background import BackgroundScheduler
import utils

def update_data():
    #aws.update_tables()
    utils.send_email()
    print('email sent')

# Schedule a database update every day at 3am.
sched = BackgroundScheduler()

@sched.scheduled_job('cron', second=3)
def scheduled_job():
    update_data()

sched.start()


app = Flask(__name__)
cors = CORS(app)
app.config['CORS_HEADERS'] = 'Content-Type'

@app.route('/5d')
@cross_origin()
def get_5d_backtest():
    return aws.get_5d_backtest()

@app.route('/1m')
@cross_origin()
def get_1m_backtest():
    return aws.get_1m_backtest()

@app.route('/6m')
@cross_origin()
def get_6m_backtest():
    return aws.get_6m_backtest()

@app.route('/1y')
@cross_origin()
def get_1y_backtest():
    return aws.get_1y_backtest()

@app.route('/ytd')
@cross_origin()
def get_ytd_backtest():
    return aws.get_ytd_backtest()

@app.route('/max')
@cross_origin()
def get_max_backtest():
    return aws.get_max_backtest()

@app.route('/5dwsb')
@cross_origin()
def get_5d_wsb():
    return aws.get_5d_wsb_data()

@app.route('/1mwsb')
@cross_origin()
def get_1m_wsb():
    return aws.get_1m_wsb_data()

@app.route('/6mwsb')
@cross_origin()
def get_6m_wsb():
    return aws.get_6m_wsb_data()

@app.route('/1ywsb')
@cross_origin()
def get_1y_wsb():
    return aws.get_1y_wsb_data()

@app.route('/ytdwsb')
@cross_origin()
def get_ytd_wsb():
    return aws.get_ytd_wsb_data()

@app.route('/maxwsb')
@cross_origin()
def get_max_wsb():
    return aws.get_max_wsb_data()

@app.route('/history')
@cross_origin()
def get_history():
    return aws.get_history_data()