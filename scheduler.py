import aws
from apscheduler.schedulers.background import BlockingScheduler
import utils
from datetime import datetime

def update_data():
    aws.update_tables()
    utils.send_email()
    print('email sent')

update_data()

# # Schedule a database update every day at 3am.
# sched = BlockingScheduler()
#
# @sched.scheduled_job('cron', minute = '1, 21, 41')
# def scheduled_job():
#     update_data()
#
# sched.start()
