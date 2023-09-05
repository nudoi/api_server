from typing import Union
from datetime import datetime, timedelta
import sqlite3
import json
import numpy as np
from fastapi import APIRouter
from fastapi.responses import JSONResponse


router = APIRouter()


@router.get("/api/v1/latest_24h")
async def read_latest_24h_data():

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


@router.get("/api/v1/start_date={start_date}")
async def read_data(start_date: str):

    # connect to database
    conn = sqlite3.connect('../env_data.db')
    c = conn.cursor()

    # get latest 24h data from database
    c.execute("SELECT timestamp, temperature, humidity, pressure, uv_index, illuminance, altitude FROM measurements WHERE timestamp >= ?" , (start_date,))
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


@router.get("/api/v1/start_date={start_date}/end_date={end_date}")
async def read_data(start_date: str, end_date: str):

    # connect to database
    conn = sqlite3.connect('../env_data.db')
    c = conn.cursor()

    # calc end date + 1 day
    end_date = datetime.strptime(end_date, '%Y-%m-%d')
    end_date = end_date + timedelta(days=1)
    end_date = end_date.strftime('%Y-%m-%d')

    # get latest 24h data from database
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
    json_data = json.dumps(data)

    return JSONResponse(json_data)
