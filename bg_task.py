import pandas as pd
import io
import requests
import time
from datetime import datetime, timedelta
import sqlite3
import mysql.connector

# set headers for api request
headers = {
    "Authorization": "**********", # replace with your own api key
}

# fetch data from zentra cloud
def fetch_zentra_cloud_data():

    # set start and end date

    start_date = "01-01-2020"
    #start_date = datetime.datetime.now().strftime("%m-01-%Y")

    end_date = "12-31-2020"

    # get data from zentracloud api
    response = requests.get("https://zentracloud.com/api/v3/get_readings/?device_sn=z6-01391&start_date=" \
                            + start_date + "&end_date=" + end_date \
                            + "&output_format=csv&page_num=1&per_page=2000&sort_by=ascending", headers=headers)

    data = pd.read_csv(io.BytesIO(response.content), sep=',', skiprows=8)

    i = 1
    while True:
        i += 1

        time.sleep(60) # sleep for 60 seconds due to api restrictions

        response = requests.get("https://zentracloud.com/api/v3/get_readings/?device_sn=z6-01391&start_date=" \
                                + start_date + "&output_format=csv&page_num=" + str(i) \
                                + "&per_page=2000&sort_by=ascending", headers=headers)

        try:
            df = pd.read_csv(io.BytesIO(response.content), sep=',', skiprows=8)
            data = pd.concat([data, df])
        except:
            break


    # save data to sqlite database
    conn = sqlite3.connect("zentra_data_2020.db")

    try:
        data.to_sql("data", conn, if_exists="append")

    finally:
        conn.close()


    # save data to mysql database

    # replace user name & password with your own
    conn = mysql.connector.connect(user='root', password='@Namazu1', host='192.168.0.32', database='zentra')

    data.to_sql("data", conn, if_exists="append")

    # close database connection
    conn.close()

# calculate mean temperature for each day and save to mysql database
def calc_mean_temp(start_date: str, end_date: str):
    
    try:
        # connect to mysql database
        conn = mysql.connector.connect(user='root', password='@Namazu1', host='192.168.0.32', database='env_data')
        #conn = sqlite3.connect('../zentra_data_all.db')
        c = conn.cursor()

        # calc end date + 1 day
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        end_date = end_date + timedelta(days=1)
        end_date = end_date.strftime('%Y-%m-%d')

        # get data from database, see above for column names
        c.execute('''
                    SELECT datetime, value FROM data 
                    WHERE measurement = 'Air Temperature' AND datetime between %s and %s
                    ''', (start_date, end_date,))
        data = c.fetchall()

        conn.close()

        # convert data to pandas dataframe
        df = pd.DataFrame(data, columns=['datetime', 'value'])

        df['date'] = df['datetime'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').date())

        # group data by date and calculate mean
        data = df.groupby('date')['value'].mean()

        # save data to sqlite3 database
        conn = sqlite3.connect("calc_data.db")
        c = conn.cursor()

        # create table if not exists
        c.execute('''
                    CREATE TABLE IF NOT EXISTS data (
                        date TEXT PRIMARY KEY,
                        value REAL
                    )
                    ''')

        # insert date and mean temperature to database
        for index, value in data.items():
            c.execute('''
                        INSERT INTO data (date, value) VALUES (?, ?)
                        ''', (index, value,))
            conn.commit()

        # close database connection
        conn.close()

    except Exception as e:
        print(e)


if __name__ == "__main__":

    start_date = "2020-01-01"
    end_date   = "2020-12-31"

    calc_mean_temp(start_date, end_date)