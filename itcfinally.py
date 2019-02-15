#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul 12 16:44:53 2018.

@author: dmitriy
"""
# import all the necessary libraries
import argparse
from bs4 import BeautifulSoup
import datetime as dt
import pandas as pd
import requests
import time as t
from tqdm import tqdm

# construct the argument parse and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-f", "--from", required=False, type=str,
                default=None, help="Choose start date")
ap.add_argument("-t", "--to", required=False, type=str,
                default=None, help="Choose end date")
ap.add_argument("-c", "--count", required=False, type=int,
                default=None, help="Specify number of pages")
args = vars(ap.parse_args())


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


# list2 = ['18-03-2012', '21-03-2012']  # d-m-y
list2 = [args["from"], args["to"]]
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


def prep_numbers(count):
    """Prep list to parse.

    __Attributes__
        count: integer number how much pages you want to scrape.

    __Returns__
        listadres: list with data to parse.

    """
    # https://itc.ua/?s&after=10-01-2019&before=12-01-2019
    # https://itc.ua/page/3/?s&after=10-01-2019&before=12-01-2019
    # Create list which contains how many pages you want to scraping
    # numbers = list(range(list1[0], list1[1]+1))
    numbers = [i for i in range(0, count)]
    # Create an empty list for all of the page adresses
    listadres = []
    # Use for loop to fill list above with page adresses
    for i in numbers:
        if i == 0:
            example = "https://itc.ua/"
        else:
            example = "https://itc.ua/page/" + str(i+1) + "/"
        listadres.append(example)
    # Check output
    print(listadres)
    return listadres

# listadres = prep_numbers(list1)
# Create function that scraping full one page from site


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
    counts = soup.find_all("span", class_="comments part")
    # Find preface for topic
    sometext = soup.find_all("div", class_="entry-excerpt hidden-xs")
    # Category
    category = soup.find_all("span", class_="cat part text-uppercase")
    '''
    #Find full text for topic
    fulltext = soup.find_all("div", class_ = "col-xs-8 col-txt")
    #Create an empty lists
    listlinks = fulltext
    listfulltext = []
    listsources = []
    #Fill the lists above our scraping date
    # print(listlinks)
    #Fill the lists above our scraping date
    for i in range(0, 24):
        k = (
            str(listlinks[i])
            .split('" rel="bookmark">')[0]
            .replace('<div class="col-xs-8 col-txt">','')
            .replace('<h2 class="entry-title text-uppercase bold_title">','')
            .replace('<h2 class="entry-title text-uppercase "> ', '')
            .replace("[' ', ","")
            .replace('<a href="','')
            .replace("[","")
            )
        page2 = requests.get(k, headers = headers)
        soup2 = BeautifulSoup(page2.content, 'html.parser')
        d = soup2.find("div", class_ = "post-txt")
        if d is None:
            d = soup2.find("div", class_ = "container-fluid txt")
        elif len(list(set(list(d.get_text())))) == 1:
            d = soup2.find_all("div", class_ = "post-txt")
            d = d[1]
        if d is not None:
            d = d.get_text()
            d = " ".join(d.split())
        d = (
            str(d)
            .split('.articles-meta')[0]
            .split('.post-facts')[0]
            .replace(".", " . ")
            )
        listfulltext.append(d)
        k = (
            d
            .replace("Источник:","_I_")
            .replace("Источники:","_I_")
            .split("_I_")[-1]
            .replace(" . ", ".")
            .replace(" .", ".")
            )
        if k is None:
            listsources.append("")
        else:
            listsources.append(k)
        t.sleep(1.5)
    '''
    # Create an empty lists
    listtitle = []
    listtime = []
    listtimeup = []
    listauthor = []
    listcounts = []
    listsometext = []
    listcategory = []

    limit = min([len(list(title)),
                 len(list(time)),
                 len(list(timeup)),
                 len(list(timeup)),
                 len(list(author)),
                 len(list(counts)),
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
        o = counts[i].get_text().replace("\n", "").replace("\t", "")
        listcounts.append(o)
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
            "date": listtime,
            "time": listtimeup,
            "author": listauthor,
            "counts": listcounts,
            "sometext": listsometext,
            "category": listcategory,
            # "fulltext": listfulltext,
            # "listsources": listsources
            })
    # Function will return that DataFrame
    return df


def calc(listadres):
    """Take list and return df with parsed data.

    __Attributes__
        listadres: list with prepared adresses.

    __Returns__
        df: DataFrame with parsed values.

    """
    # Create an empty DataFrame
    df = pd.DataFrame()
    # Adding each new page in one DataFrame
    for c, v in tqdm(enumerate(listadres)):
        if onepage(v) is None:
            break
        else:
            t.sleep(1.5)
        df = pd.concat([df, onepage(v)], ignore_index=True)
        u = 100/(len(listadres))
        percent = int(round(u*(c+1), 2))
        print("Currently done " + str(percent) + "%")
        # listadres.append(get_search_query(list2, c+1))
        t.sleep(1.5)
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
        # u = 100/(len(listadres))
        # percent = int(round(u*(c+1), 2))
        # print("Currently done " + str(percent) + "%")
        print("Parsed {} pages".format(c+1))
        listadres.append(get_search_query(list2, c+1))
        t.sleep(1.5)
    return df


# print(df.date)

# print(df.date)

# df['date']
'''
df0 = pd.read_csv("itctray.csv")
df0 = df0.loc[:, ~df0.columns.str.contains('^Unnamed')]
# del df["date"]
print(df0.head())
#df0 = df0[df.columns]
# df0 = df0.sort_values("Date")
# df0 = df0.reset_index(drop=True)
# df0.to_csv("itctray.csv")
print(df0.dtypes)
df = pd.concat([df0,df],ignore_index=True, sort=True)
'''


def get_years(df):
    """Save parsed data from DataFrame by year in each csv file.

    __Attributes__
        df: DataFrame with parsed data.

    __Returns__
        Save each year data in following csv.

    """
    list1 = [2009, 2010, 2011, 2012, 2013, 2014, 2015, 2016, 2017, 2018]
    for i in list1:
        df = df[df['Date'].dt.year == i]
        # 3:19 PM 13/12/2018
        # Change datetime format to what we want (example above)

        df['Date'] = (
                df['Date']
                .apply(lambda x: dt.datetime.strftime(x, '%I:%M %p %d/%m/%Y'))
                )

        print(df['Date'])

        df.drop_duplicates(subset='title', inplace=True)
        print(df.dtypes)
        df = df.sort_values("date", ascending=False)
        # df = df.sort_values("Date")
        df = df.reset_index(drop=True)
        print(df['Date'])

        # Check df
        # df = df[["title","date","Date","time","author","counts","sometext",
        #           "fulltext","listsources"]]
        print(df)
        # Save DataFrame to csv
        # df.to_csv("itctray.csv")
        df.to_csv("year_" + str(i) + ".csv")
        # df.to_csv("oneyear2018.csv")
# get_years(df)


def get_one_csv(df):
    """Prepare DataFrame.

    __Attributes__
        df: DataFrame with parsed data.

    __Returns__
        df: DataFrame sorted by date abd without duplicates.

    """
    # 3:19 PM 13/12/2018
    # Change datetime format to what we want (example above)
    df['date'] = df['date'].str.strip().str.replace(" в ", "/")
    df['Date'] = pd.to_datetime(df['date'], format="%d.%m.%Y/%H:%M")
    df['Date'] = (
            df['Date']
            .apply(lambda x: dt.datetime.strftime(x, '%I:%M %p %d/%m/%Y'))
            )

    # print(df['Date'])

    df.drop_duplicates(subset='title', inplace=True)
    # print(df.dtypes)
    df = df.sort_values("date", ascending=False)
    # del df["date"]
    # df = df.sort_values("Date")
    df = df.reset_index(drop=True)
    # print(df['Date'])

    # Check df
    # df = df[["title","date","Date","time","author","counts","sometext",
    #          "fulltext","listsources"]]
    print(df.head(5))
    # Save DataFrame to csv
    df.to_csv("itctray.csv")
    # df.to_csv("all_years.csv")
    # df.to_csv("oneyear2018.csv")
    return df


if __name__ == '__main__':
    start0 = t.time()
    if args["count"] is None \
       and args["from"] is not None and args["to"] is not None:
        # search_query = get_search_query(list2, 0)
        print("1")
        listadres = prep_numbers_2(1, list2)
        df = calc2(listadres)
        get_one_csv(df)
    if args["count"] is not None \
       and args["from"] is None and args["to"] is None:
        print("2")
        listadres = prep_numbers(args["count"])
        df = calc(listadres)
    # rint(df.head(5))
    # print(df.columns)
    # print(df.dtypes)
        get_one_csv(df)
    end0 = t.time()
    elapsed_time0 = end0 - start0
    elapsed_time0 = t.strftime("%H:%M:%S", t.gmtime(elapsed_time0))
    print("Mission complete in " + str(elapsed_time0))
