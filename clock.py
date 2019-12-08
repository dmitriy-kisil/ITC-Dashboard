#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 15 18:22:53 2019.

@author: dmitriy
"""
from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess
import signal
import os

# cmd ="python3 itcfinally2.py"
# subprocess.call(cmd, shell=True)
cmd = "python3 app.py"
print("Open process for a Dash app")
dash_app_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)




sched = BlockingScheduler(timezone="Europe/Kiev")

'''
@sched.scheduled_job('interval', minutes=25)
def timed_job():
    """Select time interval."""
    print('This job is run every three minutes.')
    cmd = "python3 itcfinally2.py"
    subprocess.call(cmd, shell=True)
    print("Complete!")
'''

# timed_job()


# job = sched.scheduled_job(timed_job(), 'interval', minutes=1)
# del job


@sched.scheduled_job('cron', day_of_week='mon-sun', hour=18, minute=00)
def scheduled_job(process=dash_app_process):
    """Schedule a job."""
    print('This job is run every weekday at 5pm.')
    print('Kill process of a Dash app')
    os.killpg(os.getpgid(process.pid), signal.SIGTERM)
    cmd = "python3 itcfinally2.py"
    subprocess.call(cmd, shell=True)
    print('Create a new process of a Dash app')
    cmd = "python3 app.py"
    global dash_app_process
    dash_app_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=True, preexec_fn=os.setsid)


sched.start()
# sched.shutdown()
