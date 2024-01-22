import sys
from datetime import date
import os
import timeit
from jugaad_data.nse import bhavcopy_save, bhavcopy_fo_save
import pandas as pd
import fastavro
# pyarrow, h5py, tables -> (pytables), feather-format -> (feather), fastavro -> (avro)

# ------------------------------------ generation of data frame --------------------------------------------
def generate_dataframe(to_date, symbol):
    from jugaad_data.nse import stock_df
    df = pd.DataFrame(stock_df(symbol=symbol[:-4], from_date=date(to_date,1,16), to_date=date(2023,1,16), series="EQ"))
    df = df[[ "DATE", "OPEN", "CLOSE", "HIGH","LOW", "LTP", "VOLUME", "VALUE","NO OF TRADES"]]
    return df

# ----------------------------------------------- CSV --------------------------------------------------------
def write_csv(DATA, symbol):
    DATA.to_csv(path_or_buf = symbol + ".csv")
def read_csv(symbol):
    return pd.read_csv(symbol + ".csv")
    
# ----------------------------------------------- txt --------------------------------------------------------
def write_txt(DATA, symbol):
    DATA.to_csv(path_or_buf = symbol + ".txt" , sep = "\t")
def read_txt(symbol):
    return pd.read_csv(symbol + ".txt", sep= "\t")

# ----------------------------------------------- pickle --------------------------------------------------------
def write_pickle(DATA, symbol):
    pd.to_pickle(DATA, symbol + ".pkl")
def read_pickle(symbol):
    return pd.read_pickle(symbol + ".pkl")

# ----------------------------------------------- parquet --------------------------------------------------------
def write_parquet(DATA, symbol):
    DATA.to_parquet(symbol + ".parquet")
def read_parquet(symbol):
    return pd.read_parquet(symbol + ".parquet")

# ----------------------------------------------- h5 --------------------------------------------------------
def write_hdf5(DATA, symbol):
    DATA.to_hdf(symbol + ".h5", key = 'data', mode = 'w')
def read_hdf5(symbol):
    return pd.read_hdf(symbol + ".h5", key = 'data')

# ----------------------------------------------- feather --------------------------------------------------------
def write_feather(DATA, symbol):
    DATA.to_feather(symbol + ".feather")
def read_feather(symbol):
    return pd.read_feather(symbol + ".feather")

# ----------------------------------------------- json --------------------------------------------------------
def write_json(DATA, symbol):
    DATA.to_json(symbol + ".json")
def read_json(symbol):
    return pd.read_json(symbol + ".json")

# ----------------------------------------------- avro --------------------------------------------------------
def write_avro(DATA, symbol):
    avro_schema = {
      "type": "record",
      "name": "StockTradeRecord",
      "fields": [
        {"name": "DATE", "type": ["null", {"type": "long", "logicalType": "timestamp-millis"}]},
        {"name": "OPEN", "type": ["null", "double"]},
        {"name": "CLOSE", "type": ["null", "double"]},
        {"name": "HIGH", "type": ["null", "double"]},
        {"name": "LOW", "type": ["null", "double"]},
        {"name": "LTP", "type": ["null", "double"]},
        {"name": "VOLUME", "type": ["null", "long"]},
        {"name": "VALUE", "type": ["null", "double"]},
        {"name": "NO_OF_TRADES", "type": ["null", "long"]}
      ]
    }
    
    records = DATA.to_dict(orient='records')

    # Write Avro file
    with open(symbol + ".avro", 'wb') as file:
        fastavro.writer(file, avro_schema, records)
    file.close()
def read_avro(symbol):
    with open(symbol + '.avro', 'rb') as file:
        avro_records = list(fastavro.reader(file))

    return pd.DataFrame(avro_records)

# -------------------------------------------------------- MAIN -----------------------------------------------------------------------
def main():
    to_date = 2023-int(sys.argv[2])
    argument = sys.argv[1]
    symbol = argument + "_sol"

    DATA = generate_dataframe(to_date, symbol)

    # ------------------------------------------------------ CSV ----------------------------------------------------------------------
    avg_csv_write_time = timeit.timeit(stmt=lambda: write_csv(DATA, symbol), number=10)
    avg_csv_read_time = timeit.timeit(stmt=lambda: read_csv(symbol), number=10) # in sec
    avg_csv_size = os.path.getsize(symbol + ".csv") # in 1000 kb
    
    # ----------------------------------------------------- txt -----------------------------------------------------------------------
    avg_txt_write_time = timeit.timeit(stmt=lambda: write_txt(DATA, symbol), number=10)
    avg_txt_read_time = timeit.timeit(stmt=lambda: read_txt(symbol), number=10) # in sec
    avg_txt_size = os.path.getsize(symbol + ".txt") # in 1000 kb  
        
    # ------------------------------------------------------ pickle -------------------------------------------------------------------
    avg_pickle_write_time = timeit.timeit(stmt=lambda: write_pickle(DATA, symbol), number=10)
    avg_pickle_read_time = timeit.timeit(stmt=lambda: read_pickle(symbol), number=10) # in sec
    avg_pickle_size = os.path.getsize(symbol + ".pkl") # in 1000 kb 
    
    # ------------------------------------------------------ Parquet ------------------------------------------------------------------
    avg_parquet_write_time = timeit.timeit(stmt=lambda: write_parquet(DATA, symbol), number=10)
    avg_parquet_read_time = timeit.timeit(stmt=lambda: read_parquet(symbol), number=10) # in sec
    avg_parquet_size = os.path.getsize(symbol + ".parquet") # in 1000 kb 
    
    # ------------------------------------------------------ HDF5 ------------------------------------------------------------------
    avg_hdf5_write_time = timeit.timeit(stmt=lambda: write_hdf5(DATA, symbol), number=10)
    avg_hdf5_read_time = timeit.timeit(stmt=lambda: read_hdf5(symbol), number=10) # in sec
    avg_hdf5_size = os.path.getsize(symbol + ".h5") # in 1000 kb 
    
    # ------------------------------------------------------ Feather ------------------------------------------------------------------
    avg_feather_write_time = timeit.timeit(stmt=lambda: write_feather(DATA, symbol), number=10)
    avg_feather_read_time = timeit.timeit(stmt=lambda: read_feather(symbol), number=10) # in sec
    avg_feather_size = os.path.getsize(symbol + ".feather") # in 1000 kb 
    
    # ------------------------------------------------------ JSON ------------------------------------------------------------------
    avg_json_write_time = timeit.timeit(stmt=lambda: write_json(DATA, symbol), number=10)
    avg_json_read_time = timeit.timeit(stmt=lambda: read_json(symbol), number=10) # in sec
    avg_json_size = os.path.getsize(symbol + ".json") # in 1000 kb 
    
    # ------------------------------------------------------ AVRO ------------------------------------------------------------------
    avg_avro_write_time = timeit.timeit(stmt=lambda: write_avro(DATA, symbol), number=10)
    avg_avro_read_time = timeit.timeit(stmt=lambda: read_avro(symbol), number=10) # in sec
    avg_avro_size = os.path.getsize(symbol + ".avro") # in 1000 kb 
    
    # ORC


if __name__ == "__main__":
    main()
