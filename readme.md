# Project README

## Table of Contents
1. [Introduction](#introduction)
2. [Project Structure](#project-structure)
3. [Table Structure](#table-structure)
4. [Column Explanation](#column-explanation)
5. [Usage](#usage)
6. [Contributions](#contributions)
7. [License](#license)

## Introduction
This project involves loading, transforming, and uploading data from CSV files to a PostgreSQL database. It performs data processing and calculations on trip data, such as calculating distances, average speeds, and theoretical fuel consumption.

## Project Structure
The project consists of the following files:

- `load_data.py`: Contains a function to load data from CSV files into pandas DataFrames.
- `transform_data.py`: Contains a function to transform the loaded data and perform calculations.
- `upload_data.py`: Contains a function to upload the transformed data to a PostgreSQL database.
- `main.py`: Main script that coordinates the processes of loading, transforming, and uploading data.
- `update_database.py`: Script that performs incremental data loading into an existing table in the database.
- `config.py`: Configuration file that contains the database credentials.
- `data/`: Folder that contains the CSV data files.
- `README.md`: This README file.

## Table Structure
The project utilizes a table named `fact_trip` to store the processed trip data. The table has the following structure:

| Column Name              | Data Type     | Description                                         |
|--------------------------|---------------|-----------------------------------------------------|
| id_trip                  | NUMERIC       | Primary key                                         |
| start                    | VARCHAR(255)  | Starting location name                              |
| end_trip                 | VARCHAR(255)  | Destination location name                           |
| state                    | VARCHAR(255)  | Current state of the trip                           |
| vehicle_type             | VARCHAR(255)  | Vehicle type                                        |
| name                     | VARCHAR(255)  | Driver's full name                                  |
| start_datetime           | TIMESTAMP     | Trip start date and time                            |
| duration                 | FLOAT         | Trip duration in hours                              |
| kilometers_traveled      | FLOAT         | Distance traveled in kilometers                     |
| average_speed            | FLOAT         | Average speed in kilometers per hour                |
| theoretical_consumption  | FLOAT         | Theoretical fuel consumption per trip               |
| last_update              | DATE          | Timestamp of the last update                        |

## Column Explanation
- `id_trip`: Unique identifier for each trip.
- `start`: Starting location name.
- `end_trip`: Destination location name.
- `state`: Current state of the trip.
- `vehicle_type`: Type of vehicle used for the trip.
- `name`: Driver's full name.
- `start_datetime`: Trip start date and time.
- `duration`: Trip duration in hours.
- `kilometers_traveled`: Distance traveled in kilometers.
- `average_speed`: Average speed in kilometers per hour.
- `theoretical_consumption`: Theoretical fuel consumption per trip.
- `last_update`: Timestamp of the last data update.

## Usage
1. Clone the repository: `git clone https://github.com/your_username/project.git`
2. Set up the database credentials in the `config.py` file.
3. Ensure you have Python and the necessary dependencies installed.
4. Run the main script: `python main.py`.

## Contributions
Contributions are welcome. If you would like to contribute to this project, follow these steps:
1. Fork the repository.
2. Create a branch for your new feature: `git checkout -b new-feature`
3. Make the changes and commit: `git commit -m "Add new feature"`
4. Push your changes to the remote repository: `git push origin new-feature`
5. Open a pull request on GitHub and describe your changes.

## License
This project is licensed under the [MIT License](LICENSE).

