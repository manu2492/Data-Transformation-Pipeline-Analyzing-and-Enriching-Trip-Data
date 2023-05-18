import pandas as pd
import os


def load_data():
    """
    Load data from CSV files.

    Returns:
        A tuple containing pandas DataFrames for each loaded CSV file.
    """
    data_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../data")
    df_states = pd.read_csv(os.path.join(data_folder, "estados.csv"), sep=";")
    df_locations = pd.read_csv(os.path.join(data_folder, "locaciones.csv"), sep=";")
    df_vehicle_types = pd.read_csv(os.path.join(data_folder, "tipo_vehiculos.csv"), sep=";")
    df_trips_1 = pd.read_csv(os.path.join(data_folder, "trips_1.csv"), sep=";")

    return df_states, df_locations, df_vehicle_types, df_trips_1
