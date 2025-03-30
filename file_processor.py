import os
import pandas as pd
from apscheduler.schedulers.blocking import BlockingScheduler
from sqlalchemy import create_engine
from datetime import *

# Directory paths
INPUT_DIR = './app/in'
OUTPUT_DIR = './app/out'
DB_URI = 'sqlite:///app.db'

# Set up the database engine
engine = create_engine(DB_URI)


# Function to load data into the database
def load_data_into_db(file_path, tablename):
    try:
        print(file_path)
        # Load data
        data = pd.read_csv(file_path)

        # Insert csv into database
        data.to_sql(tablename, con=engine, if_exists='append', index=False)
        print(f"Data from {file_path} loaded into database.")
    except Exception as e:
        print(f"Error loading data from {file_path}: {e}")


# Function to process files
def process_files():
    files = os.listdir(INPUT_DIR)
    timestamp = datetime.now()

    # Process each file in the /app/in directory
    for file in files:
        if file.endswith('.csv') and file.startswith('AquaItems'):
            file_path = os.path.join(INPUT_DIR, file)

            # Load the data into the database
            load_data_into_db(file_path, 'AquaItems')
            # Move processed file to archive
            processed_path = os.path.join(INPUT_DIR, 'processed', file+'_'+timestamp.strftime("%Y%b%d%H%M%S")+'_processed.csv')
            os.rename(file_path, processed_path)

        elif file.endswith('.csv') and file.startswith('AquaOrders'):
            file_path = os.path.join(INPUT_DIR, file)

            # Load the data into the database
            load_data_into_db(file_path, 'AquaOrders')

            # Move processed file to archive
            processed_path = os.path.join(INPUT_DIR, 'processed', file+'_'+timestamp.strftime("%Y%b%d%H%M%S")+'_processed.csv')
            os.rename(file_path, processed_path)

    # Process and generate report
    generate_report()


# Function to generate report from database
def generate_report():
    try:
        # Retrieve data from database
        query = "SELECT distinct o.order_id, i.item_name,(i.quantity - o.quantity) RemainingQuantity, o.amount, " \
                "o.order_date FROM AquaItems i " \
                "inner join AquaOrders o ON i.item_id = o.item_id"
        df = pd.read_sql(query, con=engine)
        print(df)

        # sort dataframe
        df_transformed = df.sort_values('order_id')

        # Generate output report as CSV
        output_file_path = os.path.join(OUTPUT_DIR, f'report_sales.csv')
        df_transformed.to_csv(output_file_path)
        print(f"Report generated: {output_file_path}")

    except Exception as e:
        print(f"Error generating sales report': {e}")


# Set up a scheduler to run process_files periodically (every 1 minute)
scheduler = BlockingScheduler()
scheduler.add_job(process_files, 'interval', minutes=1)

# Start the scheduler
scheduler.start()
