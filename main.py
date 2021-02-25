import os
import etl as e
import argparse
import time
import sys
import pandas as pd
from geopandas import GeoDataFrame
from shapely.geometry import Point

# setting some variables for loading data into database

DB_SCHEMA = "public"
TABLE_1 = "mines"
TABLE_2 = "states"
TABLE_3 = "companies"
TABLE_4 = "minerals"

#setting paths where the data will be stored after extraction and transformation

DOWNLOAD_DIR = "data/original"
PROCESSED_DIR = "data/processed"

# Data Extraction  from the web

def extraction(config: dict) -> None:
    """ Runs extraction

        Args:
            config (str): configuration dictionary
    """
    e.info("EXTRACTION: START DATA EXTRACTION")
    url = config["url"]
    url_1 = config["url2"] 
    fname = config["fname"]
    fname_1 = config["fname1"]
    fname = f"{DOWNLOAD_DIR}/{fname}"
    fname_1 =f"{DOWNLOAD_DIR}/{fname_1}"
    e.info("EXTRACTION: DOWNLOADING DATA")
    e.download_data(url, fname)
    e.download_data(url_1, fname_1)
    e.info("EXTRACTION: COMPLETED")
    
    #sys.exit(0)

# Data Transformation

def transformation(config: dict) -> None:
    """Runs transformation

    Args:
        config (dict): [description]
    """
    e.info("TRANSFORMATION: START TRANSFORMATION")
    e.info("TRANSFORMATION: READING DATA")
    fname = config["fname"]
    fname_1 = config["fname1"]
    fname_2 = config["fname2"]
    fname_3 = config["fname3"]
    
    # Reading the data downloaded
 
    e.info("TRANSFORMATION: Reading First Table (Mines)")
    df = e.read_csv(f"{DOWNLOAD_DIR}/{fname}")
    
    e.info("TRANSFORMATION: Reading Second Table (US States)")
    df_1 = e.read_csv(f"{DOWNLOAD_DIR}/{fname_1}")
    
    # printing some features of the data
    
    a = df.head()
    print ("THOSE ARE THE FIRST ROWS IN THE MINES DATA:" +'\n',a)
    
    b = df.columns
    print ("THE COLUMNS IN MINES DATA ARE: " +'\n',b)
    
    c = df.dtypes
    print ("THE DATA TYPE FOR EACH COLUMN IN MINES DATA :" +'\n',c)
    
    nrows, ncols = df.shape
    print("# OF ROWS AND COLUMNS THE MINES DATA: ", nrows, ncols)
    
    e.info("TRANSFORMATION:  SOME DATA STATISTICS FROM MINES DATA")

    print ("NUMBER OF MINES PER COMMODITY:" +'\n',df['COMMODITY'].value_counts())
    print ("NUMBER OF MINES PER STATE:" +'\n',df['STATE_LOCA'].value_counts())
    print ("NUMBER OF MINES PER COMPANY:" +'\n',df['COMPANY_NA'].value_counts())
    
    print ("NUMBER OF COPPER MINES: ",(df['COMMODITY']=='Copper').sum())
    print ("NUMBER OF GOLD MINES: ",(df['COMMODITY']=='Gold').sum())
    print ("NUMBER OF IRON MINES: ",(df['COMMODITY']=='Iron').sum())

    # Now working on States table 
    print ("THE COLUMNS IN THE SATES DATA ARE: " +'\n',(df_1.columns))

    # calculate state's area in square kilometers
    
    df_1['area_sq_km'] = df_1.apply(lambda row: (row.area_sqmi) / 0.3861, axis=1)

    print ("AREA IN KM SQUARE INSERTED IN STATES DATA:  " +'\n',(df_1.columns))


    # spliting tables acording to the data model (four tables)

    e.info("TRANSFORMATION: SPLITING TABLES ACCORDING TO THE DATA MODEL")

    cols_mines = config["columns_mines"]
    cols_state = config["columns_states"]
    cols_compa = config["columns_companies"]
    cols_miner = config["columns_minerals"]

    # Adding new column in Mines data : id_mine
    df["id_mine"] = df.index + 1

    # merging the two dataframes or tables

    merged_inner = pd.merge(left=df, right=df_1, left_on='STATE_LOCA', right_on='state')
    print("# OF ROWS AND COLUMNS IN THE DATA: ", merged_inner.shape)
    
    print (merged_inner)

    # Creating the geometry for the table using the coordinates

    e.info("TRANSFORMATION: CREATING THE GEOMETRY - POINTS LAYER")              

    geometry = [Point(xy) for xy in zip(merged_inner.LONGITUDE, merged_inner.LATITUDE)]
    
    gdf = GeoDataFrame(merged_inner, crs="EPSG:4326", geometry=geometry)
    print ("NOW WE HAVE A GEODATAFRAME " +'\n', gdf)

    df_mines = gdf[cols_mines]
    df_states = gdf[cols_state]
    df_compa = gdf[cols_compa]
    df_miner = gdf[cols_miner]

    e.info("TRANSFORMATION: SUBSETTING DONE")
    e.info("TRANSFORMATION: SAVING TRANSFORMED DATA")
    e.write_csv(df_mines, fname=f"{PROCESSED_DIR}/{fname}", sep=",")
    e.write_csv(df_states, fname=f"{PROCESSED_DIR}/{fname_1}", sep=",")
    e.write_csv(df_compa, fname=f"{PROCESSED_DIR}/{fname_2}", sep=",")
    e.write_csv(df_miner, fname=f"{PROCESSED_DIR}/{fname_3}", sep=",")

    e.info("TRANSFORMATION: SAVED")
    e.info("TRANSFORMATION: COMPLETED")


def load(config: dict, chunksize: int=1000) -> None:
    """Runs load

    Args:
        config (dict): configuration dictionary
        chunksize (int): the number of rows to be inserted at one time
    """
   
    #setting variables for the tables created
    try:
        fname = config["fname"]
        fname1 = config["fname1"]
        fname2 = config["fname2"]
        fname3 = config["fname3"]

    # Establishing Database connection

        db = e.DBController(**config["database"])

    #reading the four tables created

        e.info("LOAD: READING DATA")
        df_1 = e.read_csv(f"{PROCESSED_DIR}/{fname}")
        df_2 = e.read_csv(f"{PROCESSED_DIR}/{fname1}")
        df_3 = e.read_csv(f"{PROCESSED_DIR}/{fname2}")
        df_4 = e.read_csv(f"{PROCESSED_DIR}/{fname3}")

    # inserting data into postgresql

        e.info("LOAD: DATA READ")
        e.info("LOAD: INSERTING DATA INTO DATABASE")
        db.insert_data(df_1, DB_SCHEMA, TABLE_1, chunksize=chunksize)
        db.insert_data(df_2, DB_SCHEMA, TABLE_2, chunksize=chunksize)
        db.insert_data(df_3, DB_SCHEMA, TABLE_3, chunksize=chunksize)
        db.insert_data(df_4, DB_SCHEMA, TABLE_4, chunksize=chunksize)

        e.info("LOAD: DONE")
    except Exception as err:
        e.die(f"LOAD: {err}")

# function for add config_file

def parse_args() -> str:
    """ Reads command line arguments

        Returns:
            the name of the configuration file
    """
    parser = argparse.ArgumentParser(description="GPS: ETL working example")
    # setting default value of the config_file
    parser.add_argument("--config_file", default="C:/EDMUNDO/CODE/ETL-working-project/config/01.yml")
    args = parser.parse_args()

    return args.config_file

def time_this_function(func, **kwargs) -> str:
    """ Times function `func`

        Args:
            func (function): the function we want to time

        Returns:
            a string with the execution time
    """
    import time
    t0 = time.time()
    func(**kwargs)
    t1 = time.time()
    return f"'{func.__name__}' EXECUTED IN {t1-t0:.3f} SECONDS"

def main(config_file: str) -> None:
    """Main function for ETL

    Args:
        config_file (str): configuration file
    """
    # reading the config_file
    config = e.read_config(config_file)
    # message about the operation time for extraction
    msg = time_this_function(extraction, config=config)
    e.info(msg)
    # calling the function extraction
    extraction(config)
    # calling the function Tranformation
    transformation(config)
    # calling the function Load
    load(config, chunksize=10000)
    msg = time_this_function(transformation, config=config)
    e.info(msg)
    msg = time_this_function(load, config=config, chunksize=1000)
    e.info(msg)


if __name__ == "__main__":
 
    config_file = parse_args()
    main(config_file)
