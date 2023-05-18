from sqlalchemy import create_engine
from config import DB_NAME,PASSWORD,USER


def upload_data(df_to_upload):
    """
    Uploads data from a DataFrame to a PostgreSQL database.

    Args:
        df_to_upload (pandas.DataFrame): DataFrame containing the data to upload.

    """
    # Establish connection with the PostgreSQL database
    engine = create_engine(f'postgresql://{USER}:{PASSWORD}@localhost:5432/{DB_NAME}')

    # Define the schema and data types for the FACT_TRIP table
    tabla_fact_trip = """
        CREATE TABLE FACT_TRIP (
        id_trip NUMERIC PRIMARY KEY,
        start VARCHAR(255),
        end_trip VARCHAR(255),
        state VARCHAR(255),
        vehicle_type VARCHAR(255),
        name VARCHAR(255),
        start_datetime TIMESTAMP,
        duration FLOAT,
        kilometers_traveled FLOAT,
        average_speed FLOAT,
        theoretical_consumption FLOAT,
        last_update DATE
    )
    """

    # Create the fact_trip table if it doesn't exist
    with engine.connect() as connection:
        connection.execute(tabla_fact_trip)

    # Save the DataFrame data to the fact_trip table
    df_to_upload.to_sql("fact_trip", engine, if_exists="append", index=False)

    # Define the vw_trip_by_hour view
    vista_trip_by_hour = """
        CREATE OR REPLACE VIEW vw_trip_by_hour AS
        SELECT
            EXTRACT(MONTH FROM start_datetime) AS mes,
            CASE
                WHEN EXTRACT(HOUR FROM start_datetime::time) >= 7 AND EXTRACT(HOUR FROM start_datetime::time) <= 13 THEN 'MAÑANA'
                WHEN EXTRACT(HOUR FROM start_datetime::time) >= 13 AND EXTRACT(HOUR FROM start_datetime::time) <= 18 THEN 'TARDE'
                WHEN EXTRACT(HOUR FROM start_datetime::time) >= 18 AND EXTRACT(HOUR FROM start_datetime::time) <= 22 THEN 'NOCHE'
                ELSE 'OTRO'
            END AS turno,
            EXTRACT(HOUR FROM start_datetime::time) AS hora,
            COUNT(*) AS cantidad_viajes,
            SUM(kilometers_traveled) AS km_acumulados
        FROM
            fact_trip ft
        WHERE
            state = 'completo'
        GROUP BY
            EXTRACT(MONTH FROM start_datetime),
            turno,
            EXTRACT(HOUR FROM start_datetime::time)
    """

    # Create the vw_trip_by_hour view
    with engine.connect() as connection:
        connection.execute(vista_trip_by_hour)

    # Close the connection with the database
    engine.dispose()
