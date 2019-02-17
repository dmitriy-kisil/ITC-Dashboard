#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb 13 18:22:53 2019.

@author: dmitriy
"""
# import all the necessary libraries

# from bs4 import BeautifulSoup
# import datetime as dt

# import requests

# from tqdm import tqdm
# import gc
# import argparse
import ast
import configparser
from datetime import datetime
import pandas as pd
import pygsheets
import subprocess
import time as t


def get_config():
    """Get config  data."""
    config = configparser.ConfigParser()
    config.read('config.ini')
    # Our json
    oauth_file = config['DEFAULT']['oauth_file']
    # Name of sheet
    sheet_name = config['DEFAULT']['sheet_name']
    # Worksheet Name
    wks_name = config['DEFAULT']['wks_name']
    # That`s list with words, which contents we need to extract
    # in appropriate list and added new if we found
    requirements = config['DEFAULT']['requirements']
    column_order = config['DEFAULT']['column_order']
    # Exract list from strings
    requirements = ast.literal_eval(requirements)
    column_order = ast.literal_eval(column_order)

    return oauth_file, sheet_name, wks_name


def connect_to_sheet(oauth_file, sheet_name, wks_name):
    """Connect to sheet."""
    gcl = pygsheets.authorize(client_secret=oauth_file)
    sh = gcl.open(sheet_name)
    # wks means worksheet, sh means spreadsheet, gc means google client
    # gc = pygsheets.authorize(outh_file=oauth_file)
    # sh = gc.open(sheet_name)
    wks = sh.worksheet_by_title(wks_name)
    return wks


def get_df(wks):
    """Get df."""
    # Here we get dataframe from google spreadsheets
    df = wks.get_as_df()
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def get_dates(df):
    """Get last date and date today."""
    today_date = datetime.now().strftime('%d-%m-%Y')
    print(today_date)
    df["date3"] = pd.to_datetime(df["Date"], format="%I:%M %p %d/%m/%Y")
    df["date3"] = df["date3"].dt.strftime('%d-%m-%Y')
    last_date = df["date3"].tolist()[0]
    print(last_date)
    del df["date3"]
    return last_date, today_date


def save_to_place(wks, wks_name, df):
    """Save df to spreadsheet."""
    wks.clear()
    wks.rows = len(df)
    wks.cols = len(df.columns)
    wks.set_dataframe(df, (1, 1))


def get_csv():
    """Get csv."""
    df = pd.read_csv("itctray.csv", index_col=0)
    return df


def merge(new_data, old_data):
    """Merge two df."""
    merged_data = pd.concat([old_data, new_data],
                            ignore_index=True)
    print(merged_data.columns)
    print(merged_data.dtypes)
    merged_data = (merged_data.applymap(
                               lambda x: x.strip() if type(x) is str else x
                                       ))
    merged_data.drop_duplicates(subset='title', inplace=True)
    merged_data["date3"] = pd.to_datetime(merged_data["Date"],
                                          format="%I:%M %p %d/%m/%Y")
    merged_data.sort_values(by="date3", ascending=False, inplace=True)
    merged_data.reset_index(drop=True, inplace=True)
    del merged_data["date3"]
    merged_data.to_csv("itctray3.csv")
    return merged_data


if __name__ == '__main__':
    print("Begin")
    start0 = t.time()
    print("Get config data")
    oauth_file, sheet_name, wks_name = get_config()
    print("Connect to Google Spreadsheet")
    wks = connect_to_sheet(oauth_file, sheet_name, wks_name)
    print("Get old df from spreadsheet")
    df1 = get_df(wks)
    print("Get datetime now and last date from old df")
    last_date, today_date = get_dates(df1)
    print("Parse data")
    cmd = "python3 itcfinally.py -f {} -t {}".format(last_date, today_date)
    subprocess.call(cmd, shell=True)
    print("Download new df from csv to memory")
    df2 = get_csv()
    print("Merge two df in one")
    df = merge(df2, df1)
    print("DataFrame saved in {}".format(str(wks_name)))
    # save_to_place(wks, wks_name, df)
    end0 = t.time()
    elapsed_time0 = end0 - start0
    elapsed_time0 = t.strftime("%H:%M:%S", t.gmtime(elapsed_time0))
    print("Mission complete in " + str(elapsed_time0))
    print("Done")
