#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 12 16:44:53 2018.

@author: dmitriy
"""
# import all the necessary libraries
from bs4 import BeautifulSoup
import datetime as dt
from datetime import datetime
from dotenv import load_dotenv, find_dotenv
import os
import pandas as pd
import psycopg2
import requests
import time as t

from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

load_dotenv()


def catch_error(something):
    """Try catch an exception.

    __Attributes__
        something: Where we will search for Exception.

    __Returns__
        something if not error or "" if error is.

    """
    try:
        something
    except IndexError:
        something = ""
        print("An Index Error!")
        return something
    else:
        # print(something)
        return something


def create_conn():
    """Create connection to PostgreSQL DB."""
    DATABASE_URL = os.environ['DATABASE_URL']
    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()
    return conn, cursor


def create_table(conn, cursor):
    """Create a table if not exists in DB."""
    sql_query = """CREATE TABLE IF NOT EXISTS messages
    (
      id serial NOT NULL,
      title character varying(255),
      date4 date,
      time3 character varying(50),
      author character varying(50),
      counts int,
      sometext character varying(512),
      category character varying(50),
      date2 character varying(255),
      CONSTRAINT persons_pkey PRIMARY KEY (id)
    )"""
    cursor.execute(sql_query)


def fill_table(conn, cursor, csv_file_name):
    """Fill empty table wth data from csv."""
    sql = """COPY messages(title, date4, time3, author, counts,
     sometext, category, date2) FROM STDIN DELIMITER ',' CSV HEADER;"""
    cursor.copy_expert(sql, open(csv_file_name, "r"))


def check_table(conn, cursor):
    """See data in our table as df."""
    df = pd.read_sql("SELECT * FROM messages", conn)
    print(df.head(5))
    print(len(df))
    return df


def delete_duplicates(conn, cursor):
    """Delete duplicates in table."""
    # Delete duplicates
    sql_query = """DELETE FROM messages
    WHERE id IN
        (SELECT id
        FROM
            (SELECT id,
             ROW_NUMBER() OVER( PARTITION BY title
            ORDER BY  id ) AS row_num
            FROM messages ) t
            WHERE t.row_num > 1 );"""
    cursor.execute(sql_query)


def delete_counts_null(conn, cursor):
    """Delete duplicates in table."""
    # Delete duplicates
    sql_query = """DELETE FROM messages
    WHERE counts IS NULL;"""
    cursor.execute(sql_query)


def sort_by_dates(conn, cursor):
    """Sort data by date4 column."""
    # to_char(date2, 'HH12:MI PM DD/MM/YYYY') newsdate,
    sql_query = """SELECT * FROM messages
    GROUP BY messages.id
    ORDER BY messages.date4 DESC;"""
    df = pd.read_sql(sql_query, conn)
    print(df.head(5))
    print(len(df))
    return df


def drop_table(conn, cursor):
    """Drop a table if exists."""
    sql_query = "DROP TABLE IF EXISTS messages;"
    cursor.execute(sql_query)


def save_changes(conn, cursor):
    """Commit changes to DB."""
    # Make the changes to the database persistent
    conn.commit()


def close_conn(conn, cursor):
    """Close connection to DB."""
    conn.close()
    cursor.close()


def get_count(adres):
    # options = FirefoxOptions()
    # options.add_argument("--headless")
    # caps = DesiredCapabilities.FIREFOX.copy()
    # caps['marionette'] = False
    from selenium.webdriver.firefox.options import Options
    options = Options()
    options.set_headless(headless=True)
    from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

    cap = DesiredCapabilities().FIREFOX
<<<<<<< HEAD
    cap["marionette"] = True
=======
    cap["marionette"] = False
>>>>>>> f57ca0d3b21b9a862d1ba9679c595cb946c1e02c
    binary = '/app/vendor/firefox'
    driver = webdriver.Firefox(capabilities=cap, options=options)
    # driver = webdriver.Firefox(options=options)
    driver.get(adres)
    elems = driver.find_elements_by_class_name("disqus-comment-count.a-not-img")
    list_counts = [int(i.text) for i in elems]
    driver.close()
    return list_counts


def prep_dashboard(conn, cursor):
    """Get dates from DB; needed for DatePicker in Dashboard."""
    df = sort_by_dates(conn, cursor)
    df["date4"] = pd.to_datetime(df["date2"], format="%I:%M %p %d/%m/%Y")
    last_date_string = df["date4"].dt.strftime('%d %B, %Y').tolist()[0]
    first_date_string = df["date4"].dt.strftime('%d %B, %Y').tolist()[-1]
    month_allowed = df["date4"].dt.strftime('%m-%Y').tolist()[0]
    df["date4"] = df["date4"].dt.strftime('%Y-%m-%d')
    first_date, last_date = df["date4"].min(), df["date4"].max()
    df = check_table(conn, cursor)
    df['date4'] = pd.to_datetime(df['date2'], format="%I:%M %p %d/%m/%Y")
    df['date4'] = (
            df['date4']
            .apply(lambda x: dt.datetime.strftime(x, '%Y-%m-%d %H:%M'))
            )
    df = df.sort_values("date4", ascending=False)
    df = df.reset_index(drop=True)
    return (last_date_string, first_date_string,
            month_allowed, first_date, last_date, df)


def get_dates(df):
    """Get last date and date today."""
    # print(df.columns)
    # print(df.dtypes)
    today_date = datetime.now().strftime('%d-%m-%Y')
    print(today_date)
    df["date3"] = pd.to_datetime(df["date2"], format="%I:%M %p %d/%m/%Y")
    # df["date3"] = pd.to_datetime(df["date2"], format="%I:%M %p %m/%d/%Y")
    df["date3"] = df["date3"].dt.strftime('%d-%m-%Y')
    last_date = df["date3"].tolist()[0]
    # last_date = df["date4"].max()
    print(last_date)
    del df["date3"]
    return last_date, today_date


def prep_list_dates(last_date, today_date):
    """Prep list with dates."""
    list2 = [last_date, today_date]
    return list2


# list2 = ['18-03-2012', '21-03-2012']  # d-m-y
# list1 = [i for i in range(1, args["count"])]


def get_search_query(list2, t):
    """Create a query, that we will parse.

    __Attributes__
        list2: List, which contain start and end date.
        t: if 1 change search query (look below)

    __Returns__
        search_query: List, which contain search query.

    """
    if t > 1:
        search_query = ("https://itc.ua/page/" + str(t)
                        + "/?s&after=" + str(list2[0])
                        + "&before=" + str(list2[1])
                        )
    else:
        search_query = ("https://itc.ua/?s&after=" + str(list2[0])
                        + "&before=" + str(list2[1])
                        )
    return search_query


def prep_numbers_2(count, list2):
    """Prep list to parse.

    __Attributes__
        list2: list with dates.

    __Returns__
        listadres: list with data to parse.

    """
    # https://itc.ua/?s&after=10-01-2019&before=12-01-2019
    # https://itc.ua/page/3/?s&after=10-01-2019&before=12-01-2019
    # Create list which contains how many pages you want to scraping
    numbers = list(range(0, count))
    # Create an empty list for all of the page adresses
    listadres = []
    # Use for loop to fill list above with page adresses
    for i in numbers:
        if i == 1:
            example = ("https://itc.ua/?s&after=" + str(list2[0])
                       + "&before=" + str(list2[1]))
        else:
            example = ("https://itc.ua/page/" + str(i)
                       + "/?s&after=" + str(list2[0])
                       + "&before=" + str(list2[1])
                       )

        listadres.append(example)
    # Check output
    print(listadres)
    return listadres


def onepage(adres):
    """Take one query and get DataFrame.

    __Attributes__
        adres: one query.

    __Returns__
        df: DataFrame with parsed values.

    """
    # Use headers to prevent hide our script
    headers = {'User-Agent': 'Mozilla/5.0'}
    # Get page
    page = requests.get(adres, headers=headers)  # read_timeout=5
    # Get all of the html code
    soup = BeautifulSoup(page.content, 'html.parser')
    header = soup.find("header", class_="entry-header")
    # header_text = header.get_text()
    if header is not None:
        print("That's all!")
        return None

    # Find title the topic
    title = soup.find_all("h2", class_="entry-title")
    # Find time when topic is published
    time = soup.find_all("time", class_="published")
    # Find time when topic is updated
    timeup = soup.find_all("time", class_="screen-reader-text updated")
    # Find author the topic
    author = soup.find_all("a", class_="screen-reader-text fn")
    # Find how many comments have topic
    # counts = soup.find_all("span", class_="comments part")
    listcounts = get_count(adres)
    # counts = soup.find_all("a", class_="disqus-comment-count")
    # Find preface for topic
    sometext = soup.find_all("div", class_="entry-excerpt hidden-xs")
    # Category
    category = soup.find_all("span", class_="cat part text-uppercase")
    # Create an empty lists
    listtitle = []
    listtime = []
    listtimeup = []
    listauthor = []
    # listcounts = []
    listsometext = []
    listcategory = []

    limit = min([len(list(title)),
                 len(list(time)),
                 len(list(timeup)),
                 len(list(timeup)),
                 len(list(author)),
                 len(listcounts),
                 len(list(sometext)),
                 len(list(category))
                 ]
                )

    # Fill the lists above our scraping date
    for i in range(0, limit):
        k = title[i].get_text().replace("\n", "").replace("\t", "")
        listtitle.append(k)
        # listtitle = " ".join(title[i].get_text().split())
        # listtitle = " ".join(title[i].get_text().split())
        ll = time[i].get_text().replace("\n", "").replace("\t", "")
        listtime.append(ll)
        # listtime = " ".join(time[i].get_text().split())
        m = timeup[i].get_text().replace("\n", "").replace("\t", "")
        listtimeup.append(m)
        # listtimeup = " ".join(timeup[i].get_text().split())
        try:
            n = author[i].get_text().replace("\n", "").replace("\t", "")
        except IndexError:
            n = ""
        listauthor.append(n)
        # n = catch_error(author[i].get_text())
        # listauthor.append(n)
        '''
        o = counts[i].get_text().replace("\n", "").replace("\t", "")
        listcounts.append(o)
        '''
        # listcounts = " ".join(counts[i].get_text().split())
        try:
            p = sometext[i].get_text().replace("\n", "").replace("\t", "")
        except IndexError:
            p = ""
        listsometext.append(p)
        c = category[i].get_text()
        listcategory.append(c)
        # listcategory = " ".join(category[i].get_text().split())

    # Create DataFrame, that will contains info from lists
    df = pd.DataFrame({
            "title": listtitle,
            "date4": listtime,
            "time3": listtimeup,
            "author": listauthor,
            "counts": listcounts,
            "sometext": listsometext,
            "category": listcategory
            })
    # Function will return that DataFrame
    return df


def calc2(listadres):
    """Take list and return df with parsed data.

    __Attributes__
        listadres: list with prepared adresses.

    __Returns__
        df: DataFrame with parsed values.

    """
    # Create an empty DataFrame
    df = pd.DataFrame()
    # Adding each new page in one DataFrame
    for c, v in enumerate(listadres):
        if onepage(v) is None:
            break
        else:
            t.sleep(1.5)
        df = pd.concat([df, onepage(v)], ignore_index=True)
        print("Parsed {} pages".format(c+1))
        listadres.append(get_search_query(list2, c+1))
        t.sleep(1.5)
    return df


def get_one_csv(df):
    """Prepare DataFrame.

    __Attributes__
        df: DataFrame with parsed data.

    __Returns__
        df: DataFrame sorted by date abd without duplicates.

    """
    # 3:19 PM 13/12/2018
    # Change datetime format to what we want (example above)
    df['date4'] = df['date4'].str.strip().str.replace(" в ", "/")

    df['date2'] = pd.to_datetime(df['date4'], format="%d.%m.%Y/%H:%M")
    df['date2'] = (
            df['date2']
            .apply(lambda x: dt.datetime.strftime(x, '%I:%M %p %d/%m/%Y'))
            )

    df.drop_duplicates(subset='title', inplace=True)
    df['date4'] = pd.to_datetime(df['date2'], format="%I:%M %p %d/%m/%Y")
    df['date4'] = (
            df['date4']
            .apply(lambda x: dt.datetime.strftime(x, '%Y-%m-%d %H:%M'))
            )
    df = df.sort_values("date4", ascending=False)
    df = df.reset_index(drop=True)
    return df


def remove_csv(names):
    """Remove all csv."""
    for i in names:
        if os.path.exists(i):
            os.remove(i)
    print("Remove complete")


if __name__ == '__main__':
    print("Begin")
    start0 = t.time()

    names = ["itctray41.csv", "itctray42.csv", "itctray43.csv"]
    print("Connnect to DB")
    conn, cursor = create_conn()
    print("Create table if not exists in DB")
    create_table(conn, cursor)
    '''
    conn, cursor = create_conn()
    print("Create table if not exists in DB")
    create_table(conn, cursor)
    # print("Fill DB with data from csv")
    fill_table(conn, cursor, "itctray.csv")
    '''
    print("Get old df from DB")
    df1 = sort_by_dates(conn, cursor)
    # df1 = df1.select_dtypes(include=['object']).applymap(lambda x: x.strip() if x else x)
    df1.to_csv(names[0], index=False)
    print("Get datetime now and last date from old df")

    last_date, today_date = get_dates(df1)
    # 27.05 15.03
    print(last_date)
    print(today_date)
    # drop_table(conn, cursor)
    # save_changes(conn, cursor)
    # print("Close connection to DB")
    # close_conn(conn, cursor)
    '''
    today_date = "27-05-2019"
    last_date = "25-05-2019"
    '''
    print("Parse data")
    list2 = prep_list_dates(last_date, today_date)
    print("1")
    listadres = prep_numbers_2(1, list2)

    df = calc2(listadres)
    # print(df.date2.tolist()[:10])
    df = get_one_csv(df)
    # df = df.select_dtypes(include=['object']).applymap(lambda x: x.strip() if x else x)
    df.to_csv(names[1], index=False)

    print("Post parsed data in DB")
    fill_table(conn, cursor, names[1])
    print("Delete duplicates in DB")
    delete_duplicates(conn, cursor)
    # drop_table(conn, cursor)
    df3 = sort_by_dates(conn, cursor)
    print(df3.head(5))
    # df4 = check_table(conn, cursor)
    # print(df4.head(5))
    # df3 = df3.select_dtypes(include=['object']).applymap(lambda x: x.strip() if x else x)
    df3.to_csv(names[2], index=False)


    # df3 = df3.select_dtypes(include=['object']).applymap(lambda x: x.strip() if x else x)
    # df3.to_csv(names[2], index=False)
    # df3 = sort_by_dates(conn, cursor)
    print(df3.head(5))
    print(df3.date4.tolist()[:10])
    print("Save changes in DB")
    save_changes(conn, cursor)

    print("Close connection to DB")
    close_conn(conn, cursor)
    print("Delete csv")
    remove_csv(names)

    '''
    df = onepage('https://itc.ua/')
    print(df['counts'])
    print(df['counts'][0])
    adres = "https://itc.ua/blogs/italyanskie-inzhenery-razrabotali-chetveronogogo-robota-hyqreal-kotoromu-pod-silu-otbuksirovat-samolet/#disqus-thread/"
    # Use headers to prevent hide our script
    headers = {'User-Agent': 'Mozilla/5.0'}
    # Get page
    page = requests.get(adres, headers=headers)  # read_timeout=5
    # Get all of the html code
    soup = BeautifulSoup(page.content, 'html.parser')
    header = soup.find("div", id_="disqus-thread")
    header_text = header
    print(header_text)

    '''
    end0 = t.time()
    elapsed_time0 = end0 - start0
    elapsed_time0 = t.strftime("%H:%M:%S", t.gmtime(elapsed_time0))
    print("Mission complete in " + str(elapsed_time0))
    print("Done")
