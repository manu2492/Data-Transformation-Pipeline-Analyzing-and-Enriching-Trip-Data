import pandas as pd
import os
from datetime import datetime
from sqlalchemy import create_engine
from load_data import load_data
from transform_data import transform_data
from config import DB_NAME,PASSWORD,USER

def insert_data_incremental(df_to_upload, table_name):
    """
    Inserts data from a DataFrame into a PostgreSQL table using incremental loading.

    Args:
        df_to_upload (pandas.DataFrame): DataFrame containing the data to insert.
        table_name (str): Name of the table to insert the data into.

    """
    
    # Set up the database connection
    engine = create_engine(f'postgresql://{USER}:{PASSWORD}@localhost:5432/{DB_NAME}')

    # Retrieve the last_update value from the database
    existing_data = pd.read_sql_query('SELECT MAX(last_update) FROM "' + table_name + '"', engine)
    last_update_db = existing_data.iloc[0, 0]

    # Check if last_update_db is null
    if pd.isnull(last_update_db):
        last_update_db = pd.to_datetime('1900-01-01')  # Or any other minimum date you want to use
    else:
        last_update_db = pd.to_datetime(last_update_db)  # Convert to datetime

    # Convert the 'last_update' column to datetime format
    df_to_upload['last_update'] = pd.to_datetime(df_to_upload['last_update'])

    # Filter the newer records in the DataFrame
    new_records = df_to_upload[df_to_upload['last_update'] > last_update_db]

    # Check if there are new records to update
    if not new_records.empty:
        # Load the necessary data for transformation
        df_states, df_locations, df_vehicle_types, _ = load_data()

        # Transform the data of the new records
        df_to_upload = transform_data(df_states, df_locations, df_vehicle_types, new_records)

        # Insert the updated data into the database
        with engine.begin() as connection:
            # Avoid duplicates based on id_trip
            df_to_upload.to_sql('temp_table', connection, if_exists='replace', index=False)
            connection.execute(f'''
                INSERT INTO "{table_name}" (id_trip, start, end_trip, state, vehicle_type, name, start_datetime, duration, kilometers_traveled, average_speed, theoretical_consumption, last_update)
                SELECT t.id_trip, t.start, t.end_trip, t.state, t.vehicle_type, t.name, t.start_datetime, t.duration, t.kilometers_traveled, t.average_speed, t.theoretical_consumption, t.last_update
                FROM temp_table t
                LEFT JOIN "{table_name}" f ON t.id_trip = f.id_trip
                WHERE f.id_trip IS NULL
            ''')

        print("The records have been updated in the database.")
    else:
        print("No new records to update in the database.")

    # Update existing records
    with engine.begin() as connection:
        # Update the existing records in the table based on id_trip
        df_to_upload.to_sql('temp_table', connection, if_exists='replace', index=False)
        connection.execute(f'''
            UPDATE "{table_name}"
            SET start = t.start, end_trip = t.end_trip, state = t.state, vehicle_type = t.vehicle_type,
                name = t.name, start_datetime = t.start_datetime, duration = t.duration,
                kilometers_traveled = t.kilometers_traveled, average_speed = t.average_speed,
                theoretical_consumption = t.theoretical_consumption, last_update = t.last_update
            FROM temp_table t
            WHERE "{table_name}".id_trip = t.id_trip
        ''')

    print("The existing records have been updated in the database.")

    # Close the database connection
    engine.dispose()


data_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data")

# Read the trips_1.csv file and store the data in a DataFrame
df_trips_1 = pd.read_csv(os.path.join(data_folder, "trips_2.csv"), sep=";")

# Call the insert_data_incremental function only if there are new records to update
if not df_trips_1.empty:
    insert_data_incremental(df_trips_1, 'fact_trip')
else:
    print("No new records to update or insert.")
