from typing import Union
from datetime import datetime, timedelta
import sqlite3
import mysql.connector
import json, csv
import io
import numpy as np
import pandas as pd
from fastapi import APIRouter
from fastapi.responses import StreamingResponse, JSONResponse


router = APIRouter()


@router.get("/api/v2/latest_24h")
async def get_latest_24h_data(data: str = 'multi_env_sensor', format: str = 'csv'):

    if (data == 'multi_env_sensor'):

        # connect to database
        conn = sqlite3.connect('../env_data.db')
        c = conn.cursor()

        # get latest timestamp
        c.execute("SELECT MAX(timestamp) FROM measurements")
        latest_timestamp = c.fetchone()[0]
        latest_timestamp = datetime.strptime(latest_timestamp, '%Y-%m-%d %H:%M:%S.%f')

        # calc latest timestamp - 24h
        twenty_four_hours_ago = latest_timestamp - timedelta(hours=24)
        #print(str(twenty_four_hours_ago))

        # get latest 24h data from database
        c.execute("SELECT timestamp, temperature, humidity, pressure, uv_index, illuminance, altitude FROM measurements WHERE timestamp >= ?" , (twenty_four_hours_ago,))
        data = c.fetchall()

        # close database connection
        conn.close()

        # split data into lists
        timestamps = [row[0] for row in data]
        temperatures = [row[1] if row[1] > -50 else np.nan for row in data]
        humidity = [row[2] for row in data]
        pressure = [row[3] for row in data]
        uv_intensity = [row[4] if row[4] > 0 else 0 for row in data]
        lx_intensity = [row[5] for row in data]


        # return data in specified format
        if (format == 'csv'):

            stream = io.StringIO()

            writer = csv.writer(stream)

            writer.writerow(['timestamp', 'temperature', 'humidity', 'pressure', 'uv intensity', 'luminous intensity'])

            for ts, temp, hum, pres, uv, lx in zip(timestamps, temperatures, humidity, pressure, uv_intensity, lx_intensity):
                writer.writerow([ts, temp, hum, pres, uv, lx])

            return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        
        elif (format == 'json'):

            data = []

            for ts, temp, hum, pres, uv, lx in zip(timestamps, temperatures, humidity, pressure, uv_intensity, lx_intensity):
                entry = {
                    "timestamp": ts,
                    "temperature": temp,
                    "humidity": hum,
                    "pressure": pres,
                    "uv intensity": uv,
                    "luminous intensity": lx
                }
                data.append(entry)
                    
            # convert data to json
            json_data = json.dumps(data, indent=4)

            return JSONResponse(json_data)

        else:
            return "Invalid format specified. Valid formats are 'csv' and 'json'."

    elif(data == 'env_sensor'):

        # connect to database
        conn = sqlite3.connect('../env_sensor_data.db')
        c = conn.cursor()

        # get latest timestamp
        c.execute("SELECT MAX(timestamp) FROM weather")
        latest_timestamp = c.fetchone()[0]
        latest_timestamp = datetime.strptime(latest_timestamp, '%Y-%m-%d %H:%M:%S.%f')

        # calc latest timestamp - 24h
        twenty_four_hours_ago = latest_timestamp - timedelta(hours=24)
        #print(str(twenty_four_hours_ago))

        # get latest 24h data from database
        c.execute("SELECT timestamp, temperature, humidity, pressure FROM weather WHERE timestamp >= ?" , (twenty_four_hours_ago,))
        data = c.fetchall()

        # close database connection
        c.close()

        # split data into lists
        timestamp = [row[0] for row in data]
        #temperature = [row[1] for row in data]
        temperature = [row[1] if row[1] > -50 else np.nan for row in data]
        humidity = [row[2] if row[2] >= 0 else np.nan for row in data]
        pressure = [row[3] if row[3] > 800 else np.nan for row in data]

        # return data in specified format
        if (format == 'csv'):

            stream = io.StringIO()

            writer = csv.writer(stream)

            writer.writerow(['timestamp', 'temperature', 'humidity', 'pressure'])

            for ts, temp, hum, pres in zip(timestamp, temperature, humidity, pressure):
                writer.writerow([ts, temp, hum, pres])

            return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        
        elif (format == 'json'):

            data = []

            for ts, temp, hum, pres in zip(timestamp, temperature, humidity, pressure):
                entry = {
                    "timestamp": ts,
                    "temperature": temp,
                    "humidity": hum,
                    "pressure": pres
                }
                data.append(entry)
                    
            # convert data to json
            json_data = json.dumps(data, indent=4)

            return JSONResponse(json_data)

        else:
            return "Invalid format specified. Valid formats are 'csv' and 'json'."

        
    elif(data == 'zentra_cloud'):
        try:
            # connect to mysql database
            conn = mysql.connector.connect(user='root', password='@Namazu1', host='192.168.0.32', database='env_data')
            c = conn.cursor()

            # get latest timestamp
            c.execute("SELECT MAX(timestamp) FROM data")
            latest_timestamp = c.fetchone()[0]
            latest_timestamp = datetime.strptime(latest_timestamp, '%Y-%m-%d %H:%M:%S.%f')

            # calc latest timestamp - 24h
            twenty_four_hours_ago = latest_timestamp - timedelta(hours=24)
            #print(str(twenty_four_hours_ago))

            # get latest 24h data from database
            c.execute("SELECT timestamp, temperature FROM data WHERE timestamp >= ?" , (twenty_four_hours_ago,))
            data = c.fetchall()

            # split data into lists
            timestamps = [row[0] for row in data]
            temperatures = [row[1] if row[1] > -50 else np.nan for row in data]

            # return data in specified format
            if (format == 'csv'):

                stream = io.StringIO()

                writer = csv.writer(stream)

                writer.writerow(['timestamp', 'temperature'])

                for ts, temp in zip(timestamps, temperatures):
                    writer.writerow([ts, temp])

                return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
            
            elif (format == 'json'):

                data = []

                for ts, temp in zip(timestamps, temperatures):
                    entry = {
                        "timestamp": ts,
                        "temperature": temp
                    }
                    data.append(entry)
                        
                # convert data to json
                json_data = json.dumps(data, indent=4)

                return JSONResponse(json_data)

        finally:
            # close database connection
            conn.close()

    else:
        return "Invalid data specified."


@router.get("/api/v2/")
async def get_data(data: str = 'multi_env_sensor', format: str = 'csv', start_date: str = None, end_date: str = None):

    if (start_date == None or end_date == None):
        return "No start date specified."

    if (data == 'multi_env_sensor'):

        # connect to database
        conn = sqlite3.connect('../env_data.db')
        c = conn.cursor()

        # calc end date + 1 day
        end_date = datetime.strptime(end_date, '%Y-%m-%d')
        end_date = end_date + timedelta(days=1)
        end_date = end_date.strftime('%Y-%m-%d')

        # get data from database
        c.execute("SELECT timestamp, temperature, humidity, pressure, uv_index, illuminance, altitude FROM measurements WHERE ? <= timestamp and timestamp <= ?" , (start_date, end_date,))
        data = c.fetchall()

        # close database connection
        conn.close()

        # split data into lists
        timestamps = [row[0] for row in data]
        temperatures = [row[1] if row[1] > -50 else np.nan for row in data]
        humidity = [row[2] for row in data]
        pressure = [row[3] for row in data]
        uv_intensity = [row[4] if row[4] > 0 else 0 for row in data]
        lx_intensity = [row[5] for row in data]


        # return data in specified format
        if (format == 'csv'):

            stream = io.StringIO()

            writer = csv.writer(stream)

            writer.writerow(['timestamp', 'temperature', 'humidity', 'pressure', 'uv intensity', 'luminous intensity'])

            for ts, temp, hum, pres, uv, lx in zip(timestamps, temperatures, humidity, pressure, uv_intensity, lx_intensity):
                writer.writerow([ts, temp, hum, pres, uv, lx])

            return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
        
        elif (format == 'json'):

            data = []

            for ts, temp, hum, pres, uv, lx in zip(timestamps, temperatures, humidity, pressure, uv_intensity, lx_intensity):
                entry = {
                    "timestamp": ts,
                    "temperature": temp,
                    "humidity": hum,
                    "pressure": pres,
                    "uv intensity": uv,
                    "luminous intensity": lx
                }
                data.append(entry)
                    
            # convert data to json
            json_data = json.dumps(data, indent=4)

            return JSONResponse(json_data)

        else:
            return "Invalid format specified. Valid formats are 'csv' and 'json'."
        
    elif (data == 'env_sensor'):
        return "Not implemented yet."
    
    elif (data == 'zentra_cloud'):
        try:
            # connect to mysql database
            conn = mysql.connector.connect(user='root', password='@Namazu1', host='192.168.0.32', database='env_data')
            c = conn.cursor()

            # get data from database
            c.execute("SELECT timestamp, temperature FROM data WHERE timestamp between ? and ?" , (start_date, end_date,))
            data = c.fetchall()

            # split data into lists
            timestamps = [row[0] for row in data]
            temperatures = [row[1] if row[1] > -50 else np.nan for row in data]

            # return data in specified format
            if (format == 'csv'):

                stream = io.StringIO()

                writer = csv.writer(stream)

                writer.writerow(['timestamp', 'temperature'])

                for ts, temp in zip(timestamps, temperatures):
                    writer.writerow([ts, temp])

                return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
            
            elif (format == 'json'):

                data = []

                for ts, temp in zip(timestamps, temperatures):
                    entry = {
                        "timestamp": ts,
                        "temperature": temp
                    }
                    data.append(entry)
                        
                # convert data to json
                json_data = json.dumps(data, indent=4)

                return JSONResponse(json_data)

        finally:
            # close database connection
            conn.close()

    else:
        return "Invalid data specified."
    

@router.get("/api/v2/weather_forecast")
async def get_weather_forecast_data(format: str = 'csv'):

    # get current date
    start_date = datetime.now().date()
    # calc end date
    end_date = start_date + timedelta(days=5)

    # connect to database
    conn = sqlite3.connect('../weather_forecast.db')
    cursor = conn.cursor()

    # get data from database
    cursor.execute('''
        SELECT * FROM weather_forecast
        WHERE timestamp >= ? AND timestamp < ?
    ''', (start_date, end_date))

    forecast_data = cursor.fetchall()
    conn.close() # close database connection

    # return data in specified format
    if (format == 'csv'):

        stream = io.StringIO()

        writer = csv.writer(stream)

        writer.writerow(['timestamp', 'temperature', 'weather_description'])

        for row in forecast_data:
            id, city_name, timestamp, temperature, weather_description = row
            writer.writerow([timestamp, temperature, weather_description])

        return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
    
    elif (format == 'json'):

        data = []

        for row in forecast_data:
            id, city_name, timestamp, temperature, weather_description = row
            entry = {
                "timestamp": timestamp,
                "temperature": temperature,
                "weather_description": weather_description
            }
            data.append(entry)

        # convert data to json
        json_data = json.dumps(data, indent=4)

        return JSONResponse(json_data)
    
    else:
        return "Invalid data specified."


@router.get("/api/v2/mean_temp")
async def get_mean_temperature(data: str = 'meter_zl6', \
                               format: str = 'csv', \
                               start_date: str = None, \
                               end_date: str = None):

    if (start_date == None or end_date == None):
        return "No start date specified."
    
    '''
    mysql> show columns from data;
    +-------------------+---------+------+-----+---------+-------+
    | Field             | Type    | Null | Key | Default | Extra |
    +-------------------+---------+------+-----+---------+-------+
    | index             | int(11) | YES  | MUL | NULL    |       |
    | timestamp_utc     | int(11) | YES  |     | NULL    |       |
    | tz_offset         | int(11) | YES  |     | NULL    |       |
    | datetime          | text    | YES  |     | NULL    |       |
    | mrid              | int(11) | YES  |     | NULL    |       |
    | measurement       | text    | YES  |     | NULL    |       |
    | value             | double  | YES  |     | NULL    |       |
    | units             | text    | YES  |     | NULL    |       |
    | precision         | int(11) | YES  |     | NULL    |       |
    | port_num          | int(11) | YES  |     | NULL    |       |
    | sub_sensor_index  | int(11) | YES  |     | NULL    |       |
    | sensor_sn         | text    | YES  |     | NULL    |       |
    | sensor_name       | text    | YES  |     | NULL    |       |
    | error_flag        | int(11) | YES  |     | NULL    |       |
    | error_description | text    | YES  |     | NULL    |       |
    +-------------------+---------+------+-----+---------+-------+
    '''

    if (data == 'meter_zl6'):
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

            # convert data to pandas dataframe
            df = pd.DataFrame(data, columns=['datetime', 'value'])

            df['date'] = df['datetime'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').date())

            # group data by date and calculate mean
            data = df.groupby('date')['value'].mean()

            # split data into lists
            timestamps = data.index
            temperature = data.values

            temperatures = []

            for temp in temperature:
                temperatures.append(round(temp, 2))

            # return data in specified format
            if (format == 'csv'):

                stream = io.StringIO()

                writer = csv.writer(stream)

                writer.writerow(['timestamp', 'temperature'])

                for ts, temp in zip(timestamps, temperatures):
                    writer.writerow([ts, temp])

                return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
            
            elif (format == 'json'):

                data = []

                for ts, temp in zip(timestamps, temperatures):
                    entry = {
                        "timestamp": ts,
                        "temperature": temp
                    }
                    data.append(entry)
                        
                # convert data to json
                json_data = json.dumps(data, indent=4, default=str)

                return JSONResponse(json_data)
        
        except mysql.connector.Error as err:
            return "Database error: {}".format(err)
        
        except Exception as e:
            return "An error occured: {}".format(str(e))
        
        finally:
            # close database connection
            conn.close()

    else:
        return "Invalid data specified."
    

@router.get("/api/v2/accu_temp")
async def get_accumulated_temperature(data: str = 'meter_zl6', \
                                      format: str = 'csv', \
                                      start_date: str = None, \
                                      end_date: str = None, \
                                      offset: int = 0):

    if (start_date == None or end_date == None):
        return "No start date specified."
    
    '''
    mysql> show columns from data;
    +-------------------+---------+------+-----+---------+-------+
    | Field             | Type    | Null | Key | Default | Extra |
    +-------------------+---------+------+-----+---------+-------+
    | index             | int(11) | YES  | MUL | NULL    |       |
    | timestamp_utc     | int(11) | YES  |     | NULL    |       |
    | tz_offset         | int(11) | YES  |     | NULL    |       |
    | datetime          | text    | YES  |     | NULL    |       |
    | mrid              | int(11) | YES  |     | NULL    |       |
    | measurement       | text    | YES  |     | NULL    |       |
    | value             | double  | YES  |     | NULL    |       |
    | units             | text    | YES  |     | NULL    |       |
    | precision         | int(11) | YES  |     | NULL    |       |
    | port_num          | int(11) | YES  |     | NULL    |       |
    | sub_sensor_index  | int(11) | YES  |     | NULL    |       |
    | sensor_sn         | text    | YES  |     | NULL    |       |
    | sensor_name       | text    | YES  |     | NULL    |       |
    | error_flag        | int(11) | YES  |     | NULL    |       |
    | error_description | text    | YES  |     | NULL    |       |
    +-------------------+---------+------+-----+---------+-------+
    '''

    if (data == 'meter_zl6'):
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

            # convert data to pandas dataframe
            df = pd.DataFrame(data, columns=['datetime', 'value'])

            df['date'] = df['datetime'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d %H:%M:%S%z').date())

            # group data by date and calculate mean
            data = df.groupby('date')['value'].mean()

            # split data into lists
            timestamps = data.index
            temperature = data.values

            temperatures = []

            for temp in temperature:
                temperatures.append(round(temp, 2))

            # calculate accumulated temperature
            accu_temp = []
            accumulated_temp = 0

            for temp in temperatures:
                accumulated_temp += temp
                accumulated_temp -= offset
                accu_temp.append(round(accumulated_temp, 2))

            # return data in specified format
            if (format == 'csv'):

                stream = io.StringIO()

                writer = csv.writer(stream)

                writer.writerow(['timestamp', 'temperature'])

                for ts, temp in zip(timestamps, accu_temp):
                    writer.writerow([ts, temp])

                return StreamingResponse(iter([stream.getvalue()]), media_type="text/csv")
            
            elif (format == 'json'):

                data = []

                for ts, temp in zip(timestamps, accu_temp):
                    entry = {
                        "timestamp": ts,
                        "temperature": temp
                    }
                    data.append(entry)
                        
                # convert data to json
                json_data = json.dumps(data, indent=4, default=str)

                return JSONResponse(json_data)
        
        except mysql.connector.Error as err:
            return "Database error: {}".format(err)
        
        except Exception as e:
            return "An error occured: {}".format(str(e))
        
        finally:
            # close database connection
            conn.close()

    else:
        return "Invalid data specified."