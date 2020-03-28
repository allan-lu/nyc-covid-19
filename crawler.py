from requests import get
from io import BytesIO
from tabula import read_pdf
from datetime import date
import pandas as pd
import math


# access pdf through URL
url = "https://www1.nyc.gov/assets/doh/downloads/pdf/imm/covid-19-daily-data-summary.pdf"
response = get(url)


# convert data into a data frame
remote_file = response.content
memory_file = BytesIO(remote_file)
pdf = read_pdf(memory_file, pages=1)
df1 = pdf[0]


# clean up the columns
today = date.today()

df2 = df1.rename(columns={'.': 'Variable', 'Total Cases': today})
df2 = df2.drop(columns=['Unnamed: 0'])


# tidy the data frame
df3 = df2.copy()

df3.insert(0, "Category", None)

category = None
for index, row in df3.iterrows():
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

for index, row in df3.iterrows():
    if row['Category'] == row['Variable']:
        df3.drop(index, inplace=True)

df3 = df3.reset_index(drop=True)


# read in table generated yesterday
file_name = "covid_df.csv"
covid_hist = pd.read_csv(file_name, index_col=0)


# merge the data frames
df4 = df3.copy()
if covid_hist.columns[-1] == str(today):
    covid_hist = covid_hist.drop(columns=[str(today)])
df5 = covid_hist.join(df4[today])


# export data frame as CSV files
covid_df = df5.copy()

covid_df.to_csv(file_name)

backup_name = "covid_df_" + str(today) + ".csv"
backup_path = "Daily CSVs/"
covid_df.to_csv(backup_path + backup_name)
