import os
from datetime import date, timedelta
from crawl import crawl_decide

today = date.today()

file_dict = {'covid_cases.csv': "https://www1.nyc.gov/assets/doh/downloads/pdf/imm/covid-19-daily-data-summary.pdf",
             'covid_hosp.csv': "https://www1.nyc.gov/assets/doh/downloads/pdf/imm/covid-19-daily-data-summary-hospitalizations.pdf",
             'covid_deaths.csv': "https://www1.nyc.gov/assets/doh/downloads/pdf/imm/covid-19-daily-data-summary-deaths.pdf"}

for key in file_dict:
    df = crawl_decide(file_dict[key], key)
    df.to_csv(key)

    if key == 'covid_cases.csv':
        backup_name = "covid_summ_" + str(today) + ".csv"
        backup_path = "Backup CSVs/"
        df.to_csv(backup_path + backup_name)

        prev_date = today - timedelta(days=7)
        prev_backup = "covid_summ_" + str(prev_date) + ".csv"
        if os.path.exists(backup_path + prev_backup):
            os.remove(backup_path + prev_backup)
