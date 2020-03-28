from requests import get
from io import BytesIO
from tabula import read_pdf
from datetime import date
import pandas as pd
import math


def crawl_decide(url, file):
    response = get(url)
    today = date.today()

    # convert data into a data frame
    remote_file = response.content
    memory_file = BytesIO(remote_file)
    pdf = read_pdf(memory_file, pages=1)
    df = pdf[0]

    if "hospitalizations" in url:
        new_df = hosp_crawl(df, file)
    elif "deaths" in url:
        new_df = death_crawl(df, file)
    else:
        new_df = summary_crawl(df, file)

    # read in table created yesterday
    old_df = pd.read_csv(file, index_col=0)

    # join two tables
    if old_df.columns[-1] == str(today):
        old_df = old_df.drop(columns=[str(today)])
    return old_df.join(new_df[today])


def summary_crawl(df, file):
    # clean up the columns
    today = date.today()

    df = df.rename(columns={'.': 'Variable', 'Total Cases': today})
    df = df.drop(columns=['Unnamed: 0'])

    # tidy the data frame
    df.insert(0, "Category", None)

    category = None
    for index, row in df.iterrows():
        split_var = row['Variable'].split("-  ")
        if len(split_var) > 1:
            row['Variable'] = split_var[1]
        if type(row[today]) == str:
            split_tot = row[today].split(" (")
            if len(split_tot) > 1:
                row[today] = split_tot[0]
        elif math.isnan(row[today]):
            category = row['Variable']
        if row['Variable'] != 'Deaths':
            row['Category'] = category

    for index, row in df.iterrows():
        if row['Category'] == row['Variable']:
            df.drop(index, inplace=True)

    df = df.reset_index(drop=True)

    return df


def hosp_crawl(df, file):
    # clean up the columns
    today = date.today()

    df = df.rename(columns={'Age Group': 'Variable', 'Unnamed: 0': today})
    df = df.drop(columns=['Unnamed: 1'])

    # tidy the data frame
    df.insert(0, "Category", None)

    category = "Age Group"
    for index, row in df.iterrows():
        split_var = row['Variable'].split("-  ")
        if len(split_var) > 1:
            row['Variable'] = split_var[1]
        if type(row[today]) == str:
            split_tot = row[today].split(" (")
            if len(split_tot) > 1:
                row[today] = split_tot[0]
        elif math.isnan(row[today]):
            category = row['Variable']
        if row['Variable'] != 'Total':
            row['Category'] = category

    for index, row in df.iterrows():
        if row['Category'] == row['Variable']:
            df.drop(index, inplace=True)

    df = df.reset_index(drop=True)

    return df


def death_crawl(df, file):
    # clean up the columns
    today = date.today()

    df = df.rename(columns={'Age Group': 'Variable',
                            'Unnamed: 0': 'Yes',
                            'Unnamed: 1': 'No',
                            'Unnamed: 2': 'Pending',
                            'Unnamed: 3': 'Total'})

    # tidy the data frame
    categories = pd.Series([], dtype='object')

    category = "Age Group"
    for index, row in df.iterrows():
        if math.isnan(row['Yes']):
            category = row['Variable']
        if row['Variable'] != 'Total':
            categories[index] = category
        split_var = row['Variable'].split("- ")
        if len(split_var) > 1:
            df.loc[index, 'Variable'] = split_var[1]

    df.insert(0, "Category", categories)

    df = pd.melt(df, id_vars=['Category', 'Variable'],
                 value_vars=['Yes', 'No', "Pending", "Total"],
                 var_name='Underlying Conditions', value_name=today)

    for index, row in df.iterrows():
        if row['Category'] == row['Variable']:
            df.drop(index, inplace=True)

    df = df.reset_index(drop=True)

    return df

