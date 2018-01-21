import os
import pytz
import datetime
from apscheduler.schedulers.blocking import BlockingScheduler
import MigrateMainRMIT60

def schedule_MigrateMainRMIT60():
    #print("MigrateMainRMIT60 - run every hr between 0-6 AM..." + str(now))
    MigrateMainRMIT60.main()

if __name__ == '__main__':

    #au_tz = 'Australia/Sydney'
    au_tz = str(os.environ["TZ"])
    now = datetime.datetime.now(pytz.timezone(au_tz))
    st_date = datetime.datetime(now.year, now.month, now.day, 00, 00, 00, 000000)

    sched = BlockingScheduler()

    # Add the job
    jobMigrateMainRMIT60 = sched.add_job(schedule_MigrateMainRMIT60, 'cron', id='MigrateMainRMIT60_run_0-6_every_hr',
                                         timezone = au_tz, start_date=st_date, hour='*/1')

    # Test only - Set the end date and add the job
    #ed_date = datetime.datetime(now.year, now.month, now.day, now.hour, 00, 00, 000000) + datetime.timedelta(hours = 1)
    #jobMigrateMainRMIT60 =  sched.add_job(schedule_MigrateMainRMIT60, 'cron', id='MigrateMainRMIT60_run_upto6times_every_10_mins',
    #                                      timezone = au_tz, start_date= st_date, end_date=ed_date, minute='*/10')

    sched.start()

# To print next scheduled job use > sched.print_jobs()
# To remove job from scheduler use > jobMigrateMainRMIT60.remove()  or sched.remove_job('MigrateMainRMIT60_run_10-11_per_1_mins')
# To stop the scheduler use > sched.shutdown()
# To resume paused job > sched.resume_job('MigrateMainRMIT60_run_10-11_per_1_mins')


