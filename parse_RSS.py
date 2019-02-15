#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Dec  5 18:02:28 2018

@author: dmitriy
"""

# Import all the neccessaries libraries
import pygsheets
import logging
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import datetime as dt
import datetime
from tqdm import tqdm
import gc
import configparser
import ast
import warnings
warnings.filterwarnings("ignore")
# Save logs in test.log
logging.basicConfig(
    filename="test.log",
    level=logging.DEBUG,
    format="%(asctime)s:%(levelname)s:%(message)s"
    )


def get_config():

    config = configparser.ConfigParser()
    config.read('config.ini')
    # Our json
    oauth_file = config['DEFAULT']['oauth_file']
    # Name of sheet
    sheet_name = config['DEFAULT']['sheet_name']
    # That`s list with words, which contents we need to extract
    # in appropriate list and added new if we found
    requirements = config['DEFAULT']['requirements']
    column_order = config['DEFAULT']['column_order']
    # Exract list from strings
    requirements = ast.literal_eval(requirements)
    column_order = ast.literal_eval(column_order)
    gcl = pygsheets.authorize(outh_file=oauth_file)
    sh = gcl.open(sheet_name)
    return requirements, column_order, gcl, sh


def prep_RSS(wks_name_3):

    # wks means worksheet, sh means spreadsheet, gc means google client
    # gc = pygsheets.authorize(outh_file=oauth_file)
    # sh = gc.open(sheet_name)
    wks = sh.worksheet_by_title(wks_name_3)
    # Here we get dataframe from google spreadsheets
    df = wks.get_as_df()
    df = df.loc[:, ~df.columns.duplicated()]
    df = df[df.RSS_feed != ""]

    # Check how much duplicates are in dataframe
    all_rss = df.RSS_feed.tolist()
    print("Found RSS links at number: " + str(len(all_rss)))
    logging.debug("Found RSS links at number: " + str(len(all_rss)))
    only_unique_rss = list(set(all_rss))
    print("Found duplicates at number: "
          + str(len(all_rss) - len(only_unique_rss)))
    print("Number RSS links without duplicates: "
          + str(len(only_unique_rss)))
    logging.debug("Found duplicates at number: "
                  + str(len(all_rss) - len(only_unique_rss)))
    logging.debug("Number RSS links without duplicates: "
                  + str(len(only_unique_rss)))

    # Prep df to save
    df = df.reset_index(drop=True)
    df = df[["RSS_feed", "Source"]]
    print(df.columns)
    # Check if dataframe contain only unique RSS:
    # Length dataframe should be equal to length only_unique_rss
    print("Length of an saved df is: " + str(len(df)))
    logging.debug("Length of an saved df is: " + str(len(df)))

    # Save unique RSS links in a dataframe
    # df.to_csv("rss_feed_here.csv")
    print("Saved successfully")
    logging.debug("Saved successfully")

    source = df.Source.tolist()
    listaddresses = df.RSS_feed.tolist()
    return source, listaddresses


def calc(chunks, num, df, requirements):

    # Select one chunk (from 0 to 29). On one RSS feed usually 30 chunks
    chunk = chunks[num]
    # Here is the magic!
    # This line split content of one chunk as we need
    # Structure of chunk before split = one large str
    chunk = (
            str(chunk)
            .replace("<description><![CDATA[", "Description:: ")
            .replace("<b>", "_I_")
            .replace("</b>", "_I_")
            .replace("\n", "")
            .replace(">click to apply</a>]]></description>", "")
            .replace("<a href=", "_I_")
            .replace("<b>", "")
            .replace("</b>", "")
            .replace("\n", "")
            .replace("&lt;", "")
            .replace("b&gt;", "")
            .replace("&mdash", ",")
            .replace("/&gt;", "")
            .replace("&amp;", "")
            .replace("/a&gt", "")
            .replace("&middot", "")
            .replace("&gt;", "")
            .replace("&rdquo;", "")
            .replace("&ldquo;", "")
            .replace("&deg;", "")
            .replace("/:", ":")
            .replace("&nbsp;", "")
            .replace("<br />", "")
            .replace("&#039;", "`")
            .replace("&nbsp;", "")
            .replace("&rsquo;", "`")
            .replace("&quot;", "'")
            .replace("&ndash;", "")
            .replace("&nbull;", "")
            .replace("&bull;", "")
            .replace("&uuml", "")
            .replace("\tThe", " The")
            .replace("-", "")
            .replace(";", "")
            .replace(":", "")
            .replace('"', "")
            .replace("'", "")
            .replace("`", "")
            )
    chunk = chunk.split("_I_")
    # Structure of chunk after split:
    # ["Description I need site for my ferma", "Budget", "$ 30",
    # "Category", "IT", "Skills", "Googling", "Country", "Wakanda"].
    # For example, for column "Budget" we need to get element after him ($ 30).
    # So fill this column at a row (row==num) for 5 chunk would be:
    # df.loc[4, requirements[1]] = chunk[2].
    # But each chunk can contain or not contain elements from requirements
    # in a different order. So, in a few chunks "Budget" may be not appear
    # (column "Budget" for this chunk would be empty);
    # in other few - "Skills" and so on.
    # Because of it, in each row will be a few empty columns.
    chunk = list(chunk)
    logging.debug(chunk)
    # Create empty columns to fill further
    chunk = chunk[:-1]
    listforcolumns = chunk[1::2]
    logging.debug(listforcolumns)
    # Added a new column to dataframe each time when found a new tag
    for i in listforcolumns:
        if i not in df.columns:
            df[i] = ""
        if i not in requirements:
            requirements.append(str(i))
    # Fill dataframe with data
    # print(requirements)
    for i in range(len(requirements)):
        for k in range(len(chunk)):
            # After each operation we would see the changes in logs
            if "Description" in chunk[k]:
                chunk[k] = chunk[k].replace("Description", "")
                df.loc[num, "Description"] = " ".join(chunk[k].split())
            # elif "Skills" in chunk[k]:
                # df.loc[num, "Skills"] = " ".join(chunk[k+1].split())
            elif requirements[i] in chunk[k]:
                df.loc[num, requirements[i]] = " ".join(chunk[k+1].split())
                logging.debug(df.loc[num, requirements[i]])
    return requirements, df


def get_one_page(adres, source):

    # Define headers to prevent you from banning
    headers = {'User-Agent': ('Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6)'
                              ' AppleWebKit/537.36 (KHTML, like Gecko)'
                              ' Chrome/61.0.3163.100 Safari/537.36')}
    # Call BeautifulSoup parser as usually
    page = requests.get(adres, headers=headers)
    soup = BeautifulSoup(page.content, features="html.parser")
    logging.debug(soup.prettify())
    # Create a list to contain all content from 30 URL from one page tags
    chunks = []
    desc = soup.find_all("description")
    for i in desc:
        chunks.append(i)
    logging.debug("Length of all chunks are:")
    logging.debug(len(chunks))
    # First chunk contains All jobs as of December 03, 2018 08:39 UTC
    # not needed
    # So let's remove those
    chunks = chunks[1:]
    # Now our list contain all the necessary information without title
    logging.debug("Length of all chunks are:")
    logging.debug(len(chunks))  # need to be equal 30
    # Create a list to contain all the titles tags
    titles = []
    title = soup.find_all("title")
    for i in title:
        i = (
            str(i)
            .replace("<title><![CDATA[", "")
            .replace("]]></title>", "")
            .replace("<description><![CDATA[", "")
            .replace("<b>", "_I_")
            .replace("</b>", "_I_")
            .replace("\n", "")
            .replace(">click to apply</a>]]></description>", "")
            .replace("<a href=", "_I_")
            .replace("<b>", "")
            .replace("</b>", "")
            .replace("\n", "")
            .replace("&lt;", "")
            .replace("b&gt;", "")
            .replace("/&gt;", "")
            .replace("amp;", "")
            .replace("&amp;", "")
            .replace("/a&gt", "")
            .replace("&gt;", "")
            .replace("/:", ":")
            .replace("&nbsp;", "")
            .replace(": ", " ")
            .replace("<br />", "")
            .replace("&#039;", "`")
            .replace("&nbsp;", "")
            .replace("&quot", "")
            .replace("&ndash", "")
            .replace("&nbull;", "")
            .replace("&bull", "")
            .replace(" - Upwork", "")
            )
        titles.append(i)
    logging.debug("Length of all titles are:")
    logging.debug(len(titles))  # need to be equal 30
    # First and second title contains All jobs | upwork.com - not needed
    # So let's remove those
    titles = titles[2:]
    # Now we have all the titles, which we are needed
    logging.debug("Length of all titles are:")
    logging.debug(len(titles))
    # List for links
    links = []
    for i in soup.select('item'):
        k = (
            str(i.guid)
            .replace("</guid>", "")
            .replace("<guid>", "")
            )
        links.append(k)
    logging.debug("Length of all links are:")
    logging.debug(len(links))  # need to be equal 30
    # List for date
    date = []
    for i in soup.select('item'):
        k = (
            str(i.pubdate)
            .replace("</pubdate>", "")
            .replace("<pubdate>", "")
            .replace(" +0000", "")
            .split(", ")[1])
        date.append(k)
    logging.debug("Length of all date are:")
    logging.debug(len(date))  # need to be equal 30
    # Create DataFrame based on our lists
    df = pd.DataFrame({"Title": titles, "Link": links, "Date": date})
    df["Source"] = source
    df["Other skills"] = ""
    # Check how df look like
    logging.debug(df.head(30))
    # print(requirements)
    for d in range(len(chunks)):
        logging.debug(d+1)
        calc(chunks, d, df, requirements)
    return df


def parser(source, listaddresses, requirements):

    # Create an empty DataFrame
    df = pd.DataFrame()

    # Adding each new page in one DataFrame
    for url in tqdm(range(len(listaddresses))):
        df = pd.concat([df, get_one_page(listaddresses[url], source[url])],
                       ignore_index=True, sort=True)
        u = 100/len(listaddresses)
        percent = str(int(round((url+1)*u, 2))) + "%" + " Complete"
        logging.debug(percent)
        # print(percent)
        logging.debug("Sleep for few seconds")
        time.sleep(1.5)
    return df


def prep_df(df, column_order):

    # Replace 0 and NaN with empty space
    df.replace(0, np.nan, inplace=True)
    df.replace(np.nan, "", inplace=True)
    # Don`t need df["Posted On"] - so just delete it
    del df["Posted On"]
    new_order = column_order
    # That order we get from parsed RSS (from new dataframe)
    old_order = df.columns
    # It`s new columns from dataframe which we want to see in "Other skills"
    added_columns = [i for i in old_order if i not in new_order]
    logging.debug(added_columns)
    for i in added_columns:
        # Added info from all added_columns to df["Other skills"]
        prep_str = df["Other skills"] + ", " + df[i]
        df["Other skills"] = prep_str.str.strip(", ")
    # Change order in columns
    df = df[new_order]
    # Change df["Date"] to datetime - we need to sort all new data by date
    df['Date'] = pd.to_datetime(df['Date'])
    df = df[df['Date'] > datetime.datetime.now() - pd.to_timedelta("30day")]
    df = df.sort_values("Date")

    # 3:19 PM 13/12/2018
    # Change datetime format to what we want (example above)
    df['Date'] = (
            df['Date']
            .apply(lambda x: dt.datetime.strftime(x, '%I:%M %p %d/%m/%Y'))
            )

    # If found info from two or more similar links:
    # added tag from "Source" to each information
    df['Source'] = df.groupby('Link')['Source'].transform(', '.join)
    # Preparation to our 'Link' column
    df_link = df.Link.tolist()
    df_link_prep = []
    s1 = "https://www.upwork.com/jobs/_~"
    for i in df_link:
        if '_%7E' in i:
            s2 = (i.split("_%7E")[1].split("?")[0])
            s = s1+s2
            df_link_prep.append(s)
        else:
            df_link_prep.append("")
    df["Link"] = df_link_prep
    # Get rid off duplicates and "Unnamed: 0"
    df = df[~df.Link.duplicated(keep='first')]
    df = df.reset_index(drop=True)
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    # Check df for changes
    print(df.head(10))
    return df


def get_old_data(wks_name):

    # wks means worksheet, sh means spreadsheet, gcl means google client
    # gcl = pygsheets.authorize(outh_file=oauth_file)
    # sh = gcl.open(sheet_name)
    wks = sh.worksheet_by_title(wks_name)
    # This dataframe contain old data with all previous info
    df = wks.get_as_df()
    df['Date'] = pd.to_datetime(df['Date'])
    df = df[df['Date'] > datetime.datetime.now() - pd.to_timedelta("30day")]
    df = df.sort_values("Date")

    # 3:19 PM 13/12/2018
    # Change datetime format to what we want (example above)
    df['Date'] = (
            df['Date']
            .apply(lambda x: dt.datetime.strftime(x, '%I:%M %p %d/%m/%Y'))
            )
    return df


def check_for_duplicates(new_data, old_data):

    l1 = len(old_data)
    print("Length of an old df is: " + str(l1))
    logging.debug("Length of an old df is: " + str(l1))

    l2 = len(new_data)

    print("Length of a new df is: " + str(l2))
    logging.debug("Length of a new df is: " + str(l2))

    # Delete duplicates
    new_data.drop_duplicates(subset='Link', inplace=True)

    old_links = old_data.Link.tolist()
    new_links = new_data.Link.tolist()
    duplicate_links = []
    # Check if duplicates remain
    for i in range(len(new_links)):
        if new_links[i] in old_links:
            duplicate_links.append(new_links[i])
    if len(duplicate_links) == 0:
        print("There are no duplicates in new data")
        logging.debug("There are no duplicates in new data")
    else:
        print("Found some duplicates in number: "
              + str(len(duplicate_links)))
        logging.debug("Found some duplicates in number: "
                      + str(len(duplicate_links)))
    return new_data, old_data, duplicate_links


def merge_df(new_data, old_data, duplicate_links, column_order):

    length_new_data = len(new_data)
    # Merge two df
    merged_data = pd.concat([old_data, new_data],
                            ignore_index=True, sort=True)

    merged_data = merged_data[column_order]
    # Replace 0 to ""

    merged_data.replace(to_replace=r"0$", value="III", regex=True)
    merged_data.replace(0, np.nan, inplace=True)
    merged_data.replace(np.nan, "", inplace=True)

    length_merged_data = len(merged_data)
    return merged_data, length_new_data, length_merged_data, duplicate_links


def remove_duplicates(merged_data, length_new_data,
                      length_merged_data, duplicate_links):

    print("Remove duplicates in a number" + str(len(duplicate_links)))
    print("Total number of new values is"
          + str(length_merged_data-length_new_data))
    logging.debug("Remove duplicates in a number"
                  + str(len(duplicate_links)))
    logging.debug("Total number of new values is"
                  + str(length_merged_data-length_new_data))
    # Check if there any duplicates remain:
    uniq_links_merged = list(np.unique(merged_data.Link))
    length_uniq = len(uniq_links_merged)
    all_links_merged = merged_data["Link"].tolist()
    length_all = len(all_links_merged)
    if length_all == length_uniq:
        print("There are no duplicates left")
        logging.debug("There are no duplicates left")
    else:
        if abs(length_all-length_uniq):
            print("There are some duplicates left"
                  + str(abs(length_all-length_uniq)))
            print("Remove duplicates")
            logging.debug("There are some duplicates left"
                          + str(abs(length_all-length_uniq)))
            logging.debug("Remove duplicates")
        print(len(merged_data))
        # Remove duplicates again
        merged_data = merged_data[~merged_data.Link.duplicated()]
        # merged_data = merged_data[~merged_data.Description.duplicated()]
        print(len(merged_data))
        uniq_links_merged = list(np.unique(merged_data.Link))
        length_uniq = len(uniq_links_merged)
        all_links_merged = merged_data["Link"].tolist()
        length_all = len(all_links_merged)
        print("Check for duplicates")
        print(str(abs(length_all-length_uniq)) + "duplicates remain")
        logging.debug("Check for duplicates")
        logging.debug(str(abs(length_all-length_uniq)) + "duplicates remain")
    return merged_data


def prep_merged_data(merged_data):

    merged_data = merged_data.loc[:, ~merged_data.columns
                                  .str.contains('^Unnamed')]
    merged_data["date2"] = (
            pd.to_datetime(
                    merged_data["Date"],
                    format="%I:%M %p %d/%m/%Y"
                    )
            )
    merged_data = merged_data.sort_values("date2")
    # print(merged_data["date2"])
    merged_data = merged_data.reset_index(drop=True)
    del merged_data["date2"]
    length_of_merged_data = len(merged_data)
    print("Length of finally df is" + str(length_of_merged_data))
    logging.debug("Length of finally df is" + str(length_of_merged_data))
    return merged_data, length_of_merged_data


def save_to_place(wks_name, merged_data):

    print("DataFrame saved in" + str(wks_name))
    logging.debug("DataFrame saved in" + str(wks_name))
    wks = sh.worksheet_by_title(wks_name)
    wks.clear()
    wks.rows = len(merged_data)
    wks.cols = len(merged_data.columns)
    wks.set_dataframe(merged_data, (1, 1))


start0 = time.time()
requirements, column_order, gcl, sh = get_config()
rt = sh.worksheets()
list1 = rt
print(list1)
list2 = []
for i in list1:
    k = str(i).split(" index:")[0].replace("<Worksheet ", "").replace("'", "")
    list2.append(k)
print(list2)
list3 = [i for i in list2 if ' RSS' in i]
list4 = [i for i in list2 if ' RSS' not in i]
print(list3)
print(list4)
# wks_name Python, Scala
# wks_name_3 Python_RSS, Scala_RSS
for c in range(len(list3)):
    source, listaddresses = prep_RSS(list3[c])
    parsed_df = parser(source, listaddresses, requirements)
    new_data = prep_df(parsed_df, column_order)
    old_data = get_old_data(list4[c])
    new_data, old_data, duplicate_links = check_for_duplicates(new_data,
                                                               old_data)
    (merged_data, length_new_data, length_merged_data,
     duplicate_links) = merge_df(
             new_data, old_data, duplicate_links, column_order)
    merged_data = remove_duplicates(merged_data, length_new_data,
                                    length_merged_data, duplicate_links)
    merged_data, length_of_merged_data = prep_merged_data(merged_data)
    save_to_place(list4[c], new_data)

gc.collect()
print("Script ended successfully. Good job!")
logging.debug("Script ended successfully. Good job!")

end0 = time.time()
elapsed_time0 = end0 - start0
elapsed_time0 = time.strftime("%H:%M:%S", time.gmtime(elapsed_time0))
print("Mission complete in " + str(elapsed_time0))
