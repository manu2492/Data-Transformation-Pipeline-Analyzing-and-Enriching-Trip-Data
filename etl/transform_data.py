import pandas as pd
from geopy.distance import geodesic
from datetime import timedelta
import re


def transform_data(df_states, df_locations, df_vehicle_types, df_trips_1):
    # Fill missing values in specific columns
    df_trips_1["destino_viaje"].fillna(df_trips_1["destino_viaje"].median(), inplace=True)
    df_trips_1["estado_actual"].fillna(df_trips_1["estado_actual"].median(), inplace=True)
    df_trips_1["tipo_vehiculo"].fillna(df_trips_1["tipo_vehiculo"].median(), inplace=True)
    df_trips_1 = df_trips_1.dropna(subset=["end_date"])

    # Merge dataframes based on columns
    df_main = pd.merge(df_trips_1, df_states, left_on='estado_actual', right_on='id', how='inner')
    df_main = df_main.drop('id', axis=1)
    df_main = df_main.rename(columns={'descripcion': 'estado'})

    df_main['origen_viaje'] = df_main['origen_viaje'].map(df_locations.set_index('id')['nombre'])
    df_main['destino_viaje'] = df_main['destino_viaje'].map(df_locations.set_index('id')['nombre'])

    df_main = pd.merge(df_main, df_locations[['nombre', 'lat', 'long']], left_on='origen_viaje', right_on='nombre', how='left')
    df_main = df_main.rename(columns={'lat': 'lat_origen', 'long': 'long_origen'})

    df_main = pd.merge(df_main, df_locations[['nombre', 'lat', 'long']], left_on='destino_viaje', right_on='nombre', how='left')
    df_main = df_main.rename(columns={'lat': 'lat_destino', 'long': 'long_destino'})

    df_main = df_main.drop('nombre_y', axis=1)
    df_main = df_main.drop('nombre_x', axis=1)

    df_main = pd.merge(df_main, df_vehicle_types, left_on='tipo_vehiculo', right_on='id', how='left')
    df_main = df_main.drop('id', axis=1)

    df_main['nombre_completo'] = df_main['nombre_chofer'].str.cat(df_trips_1['apellido_chofer'], sep=' ')
    df_main = df_main.drop('nombre_chofer', axis=1)
    df_main = df_main.drop('apellido_chofer', axis=1)

    df_main['start_datetime'] = pd.to_datetime(df_main['start_date'] + ' ' + df_main['start_time'])
    df_main['end_datetime'] = pd.to_datetime(df_main['end_date'] + ' ' + df_main['end_time'])

    df_main['duration'] = df_main['end_datetime'] - df_main['start_datetime']

    # Replace commas with dots in latitude and longitude columns
    df_main['lat_origen'] = df_main['lat_origen'].str.replace(',', '.')
    df_main['long_origen'] = df_main['long_origen'].str.replace(',', '.')
    df_main['lat_destino'] = df_main['lat_destino'].str.replace(',', '.')
    df_main['long_destino'] = df_main['long_destino'].str.replace(',', '.')

    # Convert latitude and longitude columns to numeric values
    df_main['lat_origen'] = df_main['lat_origen'].astype(float)
    df_main['long_origen'] = df_main['long_origen'].astype(float)
    df_main['lat_destino'] = df_main['lat_destino'].astype(float)
    df_main['long_destino'] = df_main['long_destino'].astype(float)

    # Calculate the distance in kilometers between origin and destination coordinates
    df_main['kilometros_recorridos'] = df_main.apply(lambda row: geodesic((row['lat_origen'], row['long_origen']), (row['lat_destino'], row['long_destino'])).kilometers, axis=1)

    # Calculate the average speed in kilometers per hour
    df_main['velocidad_media'] = df_main['kilometros_recorridos'] / (df_main['duration'].dt.total_seconds() / 3600)

    def extract_liters_per_100km(string):
        """
        Extracts numeric value from a string representing liters per 100 kilometers.

        Args:
            string (str): String containing the numeric value.

        Returns:
            float or None: Extracted numeric value as a float or None if no numeric value is found.
        """
        numeric_value = re.findall(r'\d+\.?\d*', string)
        if numeric_value:
            return float(numeric_value[0])
        else:
            return None

    # Extract numeric values from columns representing fuel consumption
    df_main['consumo urbano'] = df_main['consumo urbano'].apply(extract_liters_per_100km)
    df_main['consumo a 120km/hr'] = df_main['consumo a 120km/hr'].apply(extract_liters_per_100km)

    def calculate_theoretical_consumption(row):
        """
        Calculates the theoretical fuel consumption per kilometer based on the average speed.

        The theoretical fuel consumption is calculated using a linear interpolation formula based on the average speed.
        It takes into account the urban consumption and the consumption at 120 km/hr as the minimum and maximum values,
        respectively.

        Args:
            row (Series): Row of the dataframe containing the necessary columns.

        Returns:
            float: Theoretical fuel consumption per kilometer.
        """
        speed = row['velocidad_media']
        urban_consumption = row['consumo urbano']
        highway_consumption = row['consumo a 120km/hr']

        # Define the range of speeds and corresponding fuel consumptions
        speed_min = 0
        speed_max = 120
        consumption_min = urban_consumption
        consumption_max = highway_consumption

        # Calculate the theoretical fuel consumption using linear interpolation
        theoretical_consumption = consumption_min + (speed - speed_min) * (consumption_max - consumption_min) / (speed_max - speed_min)

        # Calculate the theoretical fuel consumption per trip
        theoretical_consumption_per_trip = theoretical_consumption / 100 * row['kilometros_recorridos']

        return theoretical_consumption_per_trip

    # Calculate the theoretical fuel consumption per kilometer
    df_main['consumo_teorico'] = df_main.apply(calculate_theoretical_consumption, axis=1)

    # Rename columns
    df_main = df_main.rename(columns={'id_viaje': 'id_trip','origen_viaje': 'start', 'destino_viaje': 'end_trip',
                                      'estado': 'state', 'tipo': 'vehicle_type', 'nombre_completo': 'name',
                                      'estado_actual': 'estado_number','fecha_de_inicio': 'start_date', 
                                      'kilometros_recorridos': 'kilometers_traveled', 'velocidad_media': 'average_speed',
                                      'consumo_teorico': 'theoretical_consumption'                    
                                      })

    # Create df_to_upload DataFrame
    df_to_upload = df_main[['id_trip', 'start', 'end_trip', 'state', 'vehicle_type', 'name', 'start_datetime', 'duration', 
                        'kilometers_traveled', 'average_speed', 'theoretical_consumption', 'last_update']].copy()

    # delete rows where state = 'cancelado'
    df_to_upload = df_to_upload[df_to_upload['state'] != 'cancelado']

    # duration in seconds to hours
    df_to_upload['duration'] = (df_to_upload['duration'].dt.total_seconds() / 3600).round(2)
    # round data to 2 decimals
    df_to_upload['kilometers_traveled'] = df_to_upload['kilometers_traveled'].round(2)
    df_to_upload['average_speed'] = df_to_upload['average_speed'].round(2)
    df_to_upload['theoretical_consumption'] = df_to_upload['theoretical_consumption'].round(2)

    return df_to_upload
