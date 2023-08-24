from typing import Union
from datetime import datetime, timedelta
import sqlite3
import json, csv
import io
import numpy as np
from fastapi import APIRouter
from fastapi.responses import StreamingResponse, JSONResponse


router = APIRouter()


@router.get("/api/v2/latest_24h")
def read_latest_24h_data(data: str = 'multi_env_sensor', format: str = 'csv'):

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

    else:
        return "Invalid data specified."
    