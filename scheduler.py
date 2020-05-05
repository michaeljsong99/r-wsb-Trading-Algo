import aws
from apscheduler.schedulers.background import BlockingScheduler
import utils

def update_data():
    aws.update_tables()
    utils.send_email()
    print('email sent')

# Schedule a database update every day at 3am.
sched = BlockingScheduler()

@sched.scheduled_job('cron', hour=8)
def scheduled_job():
    update_data()

sched.start()
