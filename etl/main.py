from load_data import load_data
from transform_data import transform_data
from upload_data import upload_data

# Load data
df_states, df_locations, df_vehicle_types, df_trips_1 = load_data()

# Transform data
df_to_upload = transform_data(df_states, df_locations, df_vehicle_types, df_trips_1)

# Upload data
upload_data(df_to_upload)