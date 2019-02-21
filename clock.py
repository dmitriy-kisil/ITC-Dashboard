#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 15 18:22:53 2019.

@author: dmitriy
"""
from apscheduler.schedulers.blocking import BlockingScheduler
import subprocess

sched = BlockingScheduler()


@sched.scheduled_job('interval', minutes=25)
def timed_job():
    """Select time interval."""
    print('This job is run every three minutes.')
    cmd = "python3 savetospred.py"
    subprocess.call(cmd, shell=True)
    print("Complete!")


# timed_job()


# job = sched.scheduled_job(timed_job(), 'interval', minutes=1)
# del job


@sched.scheduled_job('cron', day_of_week='mon-fri', hour=10)
def scheduled_job():
    """Schedule a job."""
    print('This job is run every weekday at 5pm.')


sched.start()
sched.shutdown()
