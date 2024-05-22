import pandas as pd
from flask import Flask, jsonify, render_template
from flask_cors import CORS
import mysql.connector
import requests
import io
from collections import OrderedDict
from datetime import datetime

app = Flask(__name__)
CORS(app)

url = 'https://www.spdrgoldshares.com/usa/historical-data/'
download_csv_url = 'https://www.spdrgoldshares.com/assets/dynamic/GLD/GLD_US_archive_EN.csv'

def get_db_connection():
    conn = mysql.connector.connect( 
            host="ideatrade.serveftp.net", 
            user="Chaluemwut@off", 
            password="NpokG70]*APxXICy", 
            port=51410, 
            database="db_ideatrade")
    return conn

def download_csv(url):
    response = requests.get(url)
    if response.status_code == 200:
        return response.text
    else:
        print("Failed to download CSV file.")
        return None




################
data1 = {
    "Date": ["25-Nov-2004", "24-Jul-2018"],
    "GLD Close": ["HOLIDAY", "116.04"],
    "LBMA Gold Price": ["HOLIDAY", "$1228.35"],
    "NAV per GLD in Gold": ["HOLIDAY", "94.69903"],
    "NAV/share at 10.30 a.m. NYT": ["HOLIDAY", "116.3235483"],
    "Indicative Price of GLD at 4.15 p.m. NYT": ["HOLIDAY", "116.03"],
    "Mid point of bid/ask spread at 4.15 p.m. NYT#": ["HOLIDAY", "$116.03"],
    "Premium/Discount of GLD mid point v Indicative Value of GLD at 4.15 p.m. NYT": ["HOLIDAY", "AWAITED"],
    "Daily Share Volume": ["HOLIDAY", "4799827"],
    "Total Net Asset Value Ounces in the Trust as at 4.15 p.m. NYT": ["HOLIDAY", "HOLIDAY"],
    "Total Net Asset Value Tonnes in the Trust as at 4.15 p.m. NYT": ["HOLIDAY", "HOLIDAY"],
    "Total Net Asset Value in the Trust": ["HOLIDAY", "31686534549"]
}

# Sample data with "AWAITED"
data2 = {
    "Date": ["25-Nov-2004", "24-Jul-2018"],
    "GLD Close": ["116.04", "AWAITED"],
    "LBMA Gold Price": ["$1228.35", "116.04"],
    "NAV per GLD in Gold": ["94.69903", "$1228.35"],
    "NAV/share at 10.30 a.m. NYT": ["116.3235483", "94.69903"],
    "Indicative Price of GLD at 4.15 p.m. NYT": ["116.03", "116.3235483"],
    "Mid point of bid/ask spread at 4.15 p.m. NYT#": ["$116.03", "$116.03"],
    "Premium/Discount of GLD mid point v Indicative Value of GLD at 4.15 p.m. NYT": ["AWAITED", "4799827"],
    "Daily Share Volume": ["25802863.64", "25802863.64"],
    "Total Net Asset Value Ounces in the Trust as at 4.15 p.m. NYT": ["802.55", "802.55"],
    "Total Net Asset Value Tonnes in the Trust as at 4.15 p.m. NYT": ["31686534549", "31686534549"],
    "Total Net Asset Value in the Trust": ["AWAITED", "AWAITED"]
}

# Create DataFrame from sample data
df1 = pd.DataFrame(data1)
df2 = pd.DataFrame(data2)

df1.columns = df1.columns.str.strip()
df1.columns = df1.columns.str.replace('[^\w\s]', '')
df1 = df1.replace({'\$': '', ',': '','\%':'', ' ': ''}, regex=True)
df1 = df1.replace('AWAIT', 0)

df2.columns = df2.columns.str.strip()
df2.columns = df2.columns.str.replace('[^\w\s]', '')
df2 = df2.replace({'\$': '', ',': '','\%':'', ' ': ''}, regex=True)
df2 = df2.replace('AWAIT', 0)

@app.route('/')
def home():
    return render_template('index.html')


@app.route('/show_last')
def see_last_low():
    # Get the last row of the DataFrame
    last_row = df1.iloc[-1].to_dict()
    
    # Print out the last_row dictionary
    print(last_row)
    
    # Reorder the columns
    ordered_last_row = {
        "Date": last_row["Date"],
        "GLD_Close": last_row["GLD Close"],
        "LBMA_Gold_Price": last_row["LBMA Gold Price"],
        "NAV_per_GLD_in_Gold": last_row["NAV per GLD in Gold"],
        "NAV_share": last_row["NAV/share at 10.30 a.m. NYT"],  # Use .get() to avoid KeyError
        "Indicative_Price_of_GLD": last_row["Indicative Price of GLD at 4.15 p.m. NYT"],
        "Mid_point_of_bid/ask_spread": last_row["Mid point of bid/ask spread at 4.15 p.m. NYT#"],
        "Premium/Discount_of_GLD_Percentage": last_row["Premium/Discount of GLD mid point v Indicative Value of GLD at 4.15 p.m. NYT"],
        "Daily_Share_Volume": last_row["Daily Share Volume"],
        "Total_Net_Asset_Value_Ounces_in_the_Trust_as": last_row["Total Net Asset Value Ounces in the Trust as at 4.15 p.m. NYT"],
        "Total_Net_Asset_Value_Tonnes_in_the_Trust_as": last_row["Total Net Asset Value Tonnes in the Trust as at 4.15 p.m. NYT"],
        "Total_Net_Asset_Value_in_the_Trust": last_row["Total Net Asset Value in the Trust"]
    }
    
    return jsonify(ordered_last_row)


# Function to clean keys by removing spaces
def clean_keys(data):
    return {key.strip(): value for key, value in data.items()}


from datetime import datetime

@app.route('/last_row')
def get_last_row():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)
    csv_data = download_csv(download_csv_url)

    if csv_data:
        df = pd.read_csv(io.StringIO(csv_data), skiprows=6)
        df.columns = df.columns.str.strip()
    else:
        df = pd.DataFrame()

    df.columns = df.columns.str.strip()
    df.columns = df.columns.str.replace('[^\w\s]', '')
    df = df.replace({'\$': '', ',': '','\%':'', ' ': ''}, regex=True)
    df = df.replace('AWAIT', 0)
    global last_row
    last_row = df.iloc[-1].to_dict()
    try:
        # Clean keys before insertion
        last_row = clean_keys(last_row)
        # Convert date format
        last_row["Date"] = datetime.strptime(last_row["Date"], "%d-%b-%Y").strftime("%Y-%m-%d")
        
        print("Last row is: ", last_row["Date"])
        # Check if any value in the last row is to be skipped
        values_to_skip = ["HOLIDAY", "AWAITED"]
        for value in last_row.values():
            if value in values_to_skip:
                return jsonify({"message": "Skipping insertion for row with values 'HOLIDAY' or 'AWAITED'."}), 200

        # Check if there is already a record with the same date
        sql_check = "SELECT COUNT(*) as count FROM spdr WHERE Date = %s"
        cursor.execute(sql_check, (last_row["Date"],))
        result = cursor.fetchone()
        count = result["count"]
        
        if count == 0:
            # Inserting data into the database
            sql_insert = """INSERT INTO spdr (Date, GLD_Close, LBMA_Gold_Price, NAV_per_GLD_in_Gold, NAV_share,
             Indicative_Price_of_GLD, Mid_point_of_bid_ask_spread, Premium_Discount_of_GLD_Percentage,
             Daily_Share_Volume, Total_Net_Asset_Value_Ounces_in_the_Trust_as,
             Total_Net_Asset_Value_Tonnes_in_the_Trust_as, Total_Net_Asset_Value_in_the_Trust)
             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""

            cursor.execute(sql_insert, (
                last_row["Date"], 
                last_row["GLD Close"], 
                last_row["LBMA Gold Price"], 
                last_row["NAV per GLD in Gold"], 
                last_row["NAV/share at 10.30 a.m. NYT"], 
                last_row["Indicative Price of GLD at 4.15 p.m. NYT"], 
                last_row["Mid point of bid/ask spread at 4.15 p.m. NYT#"], 
                last_row["Premium/Discount of GLD mid point v Indicative Value of GLD at 4.15 p.m. NYT"], 
                last_row["Daily Share Volume"], 
                last_row["Total Net Asset Value Ounces in the Trust as at 4.15 p.m. NYT"], 
                last_row["Total Net Asset Value Tonnes in the Trust as at 4.15 p.m. NYT"], 
                last_row["Total Net Asset Value in the Trust"]
            ))
            conn.commit()
            return jsonify({"message": "Data inserted successfully."}), 200
        else:
            return jsonify({"message": "Data with the same date already exists in the database."}), 200
    except Exception as e:
        print(f"Error inserting last row into the database: {e}")
        conn.rollback()
        return jsonify({'message': 'Failed to insert last row into the database.'}), 500
    finally:
        cursor.close()
        conn.close()


######
import schedule
import time
import threading
import requests
from pytz import timezone

# Function to request the /last_row endpoint
def request_last_row():
    url = 'http://172.18.1.69:5000/last_row'  # Replace with your server's URL
    response = requests.get(url)
    if response.status_code == 200:
        print("Request sent successfully.")
    else:
        print("Failed to send request.")

# Define your desired timezone
desired_timezone = "Asia/Bangkok"  # For example, New York timezone

# Schedule the task to run every day at 08:00 AM in the desired timezone
schedule.every().day.at("08:30").do(request_last_row).tag('my_task1').timezone = timezone(desired_timezone)
time.sleep(10)
schedule.every().day.at("14:30").do(request_last_row).tag('my_task2').timezone = timezone(desired_timezone)
time.sleep(10)
schedule.every().day.at("15:00").do(request_last_row).tag('my_task3').timezone = timezone(desired_timezone)
time.sleep(10)
schedule.every().day.at("20:30").do(request_last_row).tag('my_task4').timezone = timezone(desired_timezone)
time.sleep(10)
schedule.every().day.at("02:30").do(request_last_row).tag('my_task5').timezone = timezone(desired_timezone)
# Function to run the scheduler
def run_scheduler():
    while True:
        schedule.run_pending()
        time.sleep(4)  # Sleep for 1 second before checking the schedule again

# Start the scheduler in a separate thread
scheduler_thread = threading.Thread(target=run_scheduler)
scheduler_thread.start()


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0')
