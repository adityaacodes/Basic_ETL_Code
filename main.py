import pandas as pd
import glob
import xml.etree.ElementTree as ET
from datetime import datetime
import configparser
import psycopg2
from sqlalchemy import create_engine

log_file = "log_file.txt"
target_file = "transformed_data.csv"
table_name = 'data'

config = configparser.ConfigParser()
config.read('config.ini')
password = config['secret']['PASSWORD']


def connect_to_db(password):
    # Connecting to database
    con = psycopg2.connect(database='etl_project',
                           user='postgres',
                           password=password,
                           host='127.0.0.1',
                           port='5432')
    
    # Creating Engine
    engine = create_engine(f'postgresql+psycopg2://postgres:{password}@localhost:5432/etl_project')
  
    return con, engine


# Extracting
def extract_from_csv(file_to_process):
    dataframe = pd.read_csv(file_to_process)
    return dataframe


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process, lines=True)
    return dataframe


def extract_from_xml(file_to_process):
    dataframe = pd.DataFrame(columns=["name", "height", "weight"])
    tree = ET.parse(file_to_process)
    root = tree.getroot()
    for person in root:
        name = person.find("name").text
        height = float(person.find("height").text)
        weight = float(person.find("weight").text)
        dataframe = pd.concat([dataframe, pd.DataFrame([{"name":name, "height":height, "weight":weight}])],
                              ignore_index=True)
    return dataframe


def extract():
    # create an empty data frame to hold extracted data
    extracted_data = pd.DataFrame(columns=['name', 'height', 'weight'])

    # process all csv files
    for csvfile in glob.glob("C:/Apps/ETL_Project/source/*.csv"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_csv(csvfile))], ignore_index=True)

    # process all json files
    for jsonfile in glob.glob("C:/Apps/ETL_Project/source/*.json"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_json(jsonfile))], ignore_index=True)

    # process all xml files
    for xmlfile in glob.glob("C:/Apps/ETL_Project/source/*.xml"):
        extracted_data = pd.concat([extracted_data, pd.DataFrame(extract_from_xml(xmlfile))], ignore_index=True)

    return extracted_data


# Transforming
def transform(data):
    """Convert inches to meters and round off to two decimals
        1 inch is 0.0254 meters """
    data['height'] = round(data.height * 0.0254, 2)

    """Convert pounds to kilograms and round off to two decimals 
        1 pound is 0.45359237 kilograms """
    data['weight'] = round(data.weight * 0.45359237, 2)

    return data


# Loading
def load_data_to_csv(target_file, transformed_data):
    transformed_data.to_csv(target_file)


def load_data_to_db(engine, transformed_data):
    transformed_data.to_sql(table_name, engine, if_exists='replace', index=False)


# Querying
def query(statement, con):
    return pd.read_sql(statement, con)


# Logging
def log_progress(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Year-Monthname-Day-Hour-Minute-Second
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open(log_file, 'a') as f:
        f.write(timestamp + ',' + message + '\n')


# Log the initialization of the ETL process
log_progress("ETL Job Started")

# Log the beginning of the Extraction process
log_progress("Extract phase Started")
extracted_data = extract()

# Log the completion of the Extraction process
log_progress("Extract phase Ended")

# Log the beginning of the Transformation process
log_progress("Transform phase Started")
transformed_data = transform(extracted_data)

# Log the completion of the Transformation process
log_progress("Transform phase Ended")

# Log the beginning of the Loading process
log_progress("Load phase Started")
load_data_to_csv(target_file, transformed_data)
con, engine = connect_to_db(password)
load_data_to_db(engine, transformed_data)

# Log the completion of the Loading process
log_progress("Load phase Ended")

# Example query
log_progress("Database queried")
statement = f"SELECT * FROM {table_name}"
print(query(statement, con))
log_progress("Database Querying Ended")
con.close()

# Log the completion of the ETL process
log_progress("ETL Job Ended")
