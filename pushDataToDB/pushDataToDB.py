import json
import pyodbc
import traceback
import datetime as datetime
import numpy as np

# Convert numpy data types to standard Python data types
def convert_np_types(obj):
    if isinstance(obj, np.generic):
        return obj.item()  # Using item() method for numpy scalars
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    return obj

def pushDataToSQL(dataFromSensors):

    conn = None  # Initialize conn variable
    listOfFruits = ["Apple", "Banana", "Grapes", "Lemons", "Mangoes", "Tomatoes"]

    try:
        # Connection parameters
        server = 'localhost'
        database = 'Project'
        username = 'sa'
        password = 'SqlLogin+Hyphen'

        connectionString = f'DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password};TrustServerCertificate=yes'

        # Create a connection
        # conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+','+port+';DATABASE='+database+';UID='+username+';PWD='+ password)
        conn = pyodbc.connect(connectionString)

        # Check if connection is successful
        if conn:
            print("Connected to MS SQL Server")

            # Create a cursor
            cursor = conn.cursor()

            for data in dataFromSensors:
                transitID = int(data['Transit ID'])
                destination = data['Destination']
                regionID = int(data['Region ID'])
                regionName = data['Region Name']
                start = data['Start']
                fruitID = int(data['Fruit ID'])
                sent = float(data['Sent']) if 'Sent' in data else None
                received = float(data['Received']) if 'Received' in data else None
                spoiled=None
                lossPercent=None
                if received is not None:
                    lossPercent = float(((sent-received)*100)/sent)

                for key,value in data.items():
                    if (key in listOfFruits):
                        fruitRTData = data[key]

                # Extract timestamp key
                timestamp_key = None
                for key in fruitRTData.keys():
                    try:
                        # Try parsing the key as a datetime
                        datetime.datetime.strptime(key, '%Y-%m-%d %H:%M:%S')
                        timestamp_key = key
                        break
                    except ValueError:
                        pass

                if timestamp_key:
                    print("Timestamp Key:", timestamp_key)
                else:
                    print("No timestamp key found.")

                # Extract data using timestamp key
                if timestamp_key:
                    print("Timestamp Key:", timestamp_key)
                    timestamp_data = {
                        timestamp_key: {
                            'Temp': fruitRTData[timestamp_key]['Temp'],
                            'CO2': fruitRTData[timestamp_key]['CO2'],
                            'Humidity': fruitRTData[timestamp_key]['Humidity'],
                            'Days': fruitRTData[timestamp_key]['Days'],
                            'Spoiled': fruitRTData[timestamp_key]['Spoiled'],
                            'Max Temp': fruitRTData['Max Temp'],
                            'Min Temp': fruitRTData['Min Temp'],
                            'Max Co2': fruitRTData['Max Co2'],
                            'Min Co2': fruitRTData['Min Co2'],
                            'Max Humidity': fruitRTData['Max Humidity'],
                            'Min Humidity': fruitRTData['Min Humidity']
                        }
                    }
                    spoiled = int(fruitRTData[timestamp_key]['Spoiled'])
                    print("Timestamp Data:", timestamp_data)

                # Check if the row already exists
                cursor.execute("SELECT COUNT(*) FROM Transit Where Transit_Id = ? AND Fruit_ID = ? AND \
                            Region_ID = ? AND Start = ? AND Destination = ?", \
                                transitID, fruitID, regionID, start, destination)
                row_count = cursor.fetchone()[0]

                if row_count > 0:  # Row already exists
                    # Retrieve existing JSON data from the column
                    cursor.execute("SELECT Transit_logs FROM Transit Where Transit_Id = ? AND Fruit_ID = ? AND \
                            Region_ID = ? AND Start = ? AND Destination = ?", \
                                transitID, fruitID, regionID, start, destination)
                    existing_json = cursor.fetchone()[0]

                    # Convert existing JSON string to Python dictionary
                    existing_data = json.loads(existing_json)

                    timestamp_data = json.dumps(timestamp_data, default=convert_np_types)

                    # Merge existing data with new data
                    existing_data.update(timestamp_data)

                    # Convert merged data back to JSON string
                    updated_json = json.dumps(existing_data)

                    # Update the column with the updated JSON
                    cursor.execute("UPDATE Transit SET Transit_logs = ?, Total_Qty_Sent = ?, Total_Qty_Received = ?, \
                                Loss_Percent = ?, Spoiled =? Where Transit_Id = ? AND Fruit_ID = ? AND Region_ID = ? AND Start = ? \
                                AND Destination = ?", updated_json, sent, received, lossPercent, spoiled, transitID, fruitID, \
                                    regionID, start, destination)
                else:  # Row doesn't exist, insert new row
                    # Convert new JSON data to JSON string
                    new_json = json.dumps(timestamp_data, default=convert_np_types)

                    # Insert new row with the new JSON data
                    cursor.execute("INSERT INTO Transit (Transit_id, Fruit_ID, Region_ID, Region_name, \
                                Start, Destination, Transit_logs, Total_Qty_Sent, Total_Qty_Received, Loss_Percent) \
                                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", transitID, fruitID, regionID, regionName, start, \
                                    destination, new_json, sent, received, lossPercent)
                    cursor.execute("UPDATE Transit SET Total_Qty_Sent = ?, Total_Qty_Received = ?, \
                                Loss_Percent = ?, Spoiled =? Where Transit_Id = ? AND Fruit_ID = ? AND Region_ID = ? AND Start = ? \
                                AND Destination = ?", sent, received, lossPercent, spoiled, transitID, fruitID, \
                                    regionID, start, destination)

                # Commit the transaction
                conn.commit()

                print("Data updated/inserted successfully.")


    except Exception as e:
        print(traceback.print_exc())
        print(f"Error inserting data into MS SQL Server: {e}")

    finally:
        # Close connection
        if conn:
            conn.close()
            print("MS SQL Server connection is closed")

# Example usage
if __name__ == "__main__":
    # Sample data

    # fruitData = {
    #     "2023-10-12 19:29:33":{
    #         "Temp":10,
    #         "CO2":100,
    #         "Humidity":120,
    #         "Max Temp":50,
    #         "Min Temp":20,
    #         "Max CO2":50,
    #         "Min CO2":20,
    #         "Max Humidity":50,
    #         "Min Humidity":20,
    #         "Days": 10
    #     },
    #     "2023-10-12 20:29:33":{
    #         "Temp":15,
    #         "CO2":105,
    #         "Humidity":125,
    #         "Max Temp":55,
    #         "Min Temp":25,
    #         "Max CO2":55,
    #         "Min CO2":25,
    #         "Max Humidity":55,
    #         "Min Humidity":25,
    #         "Days": 12
    #     }
    # }

    fruitData = {
      "Apple": {
        "2024-03-29 15:36:45": {
          "Temp": 125.35722788902221,
          "CO2": 423.2440926953,
          "Humidity": 81.85851013042813,
          "Days": 1,
          "Spoiled": 0
        },
        "Max Temp": 351,
        "Min Temp": 351,
        "Max Co2": 450,
        "Min Co2": 300,
        "Max Humidity": 901,
        "Min Humidity": 90
      },
      "Fruit ID": 1,
      "Transit ID": 1,
      "Region ID": 1,
      "Region Name": "CA",
      "Start": "BritishColumbia",
      "Destination": "NovaScotia",
      "Sent": 40,
      "Received":20
    }

    # Call the function to push data
    pushDataToSQL(fruitData)
