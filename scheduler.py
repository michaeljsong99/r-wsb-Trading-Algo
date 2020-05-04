import aws
from apscheduler.schedulers.background import BackgroundScheduler
import utils

def update_data():
    aws.update_tables()
    utils.send_email()
    print('email sent')

# Schedule a database update every day at 6am.
sched = BackgroundScheduler()

@sched.scheduled_job('cron', hour=7)
def scheduled_job():
    update_data()

sched.start()