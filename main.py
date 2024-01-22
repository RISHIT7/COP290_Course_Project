import sys
from datetime import date
from jugaad_data.nse import bhavcopy_save, bhavcopy_fo_save
import pandas as pd
import timeit
import os


# ------------------------------------ generation of data frame --------------------------------------------
def generate_dataframe(to_date, symbol):
    from jugaad_data.nse import stock_df
    df = pd.DataFrame(stock_df(symbol=symbol, from_date=date(to_date,1,16), to_date=date(2023,1,16), series="EQ"))
    df = df[[ "DATE", "OPEN", "CLOSE", "HIGH","LOW", "LTP", "VOLUME", "VALUE","NO OF TRADES"]]
    return df


# ----------------------------------------------- CSV --------------------------------------------------------
def write_csv(DATA, symbol):
    DATA.to_csv(path_or_buf = symbol + "_sol.csv" )
def read_csv(symbol):
    return pd.read_csv(symbol + "_sol.csv")
    
    
def main():
    to_date = 2023-int(sys.argv[2])
    symbol = sys.argv[1]

    DATA = generate_dataframe(to_date, symbol)

    # ------------------------------------------------------ CSV ----------------------------------------------------------------------
    
    avg_csv_write_time = timeit.timeit(stmt=lambda: write_csv(DATA, symbol), number=10)
    avg_csv_read_time = timeit.timeit(stmt=lambda: read_csv(symbol), number=10) # in sec
    avg_csv_size = os.path.getsize(symbol + "_sol.csv") # in 1000 kb
    
    # txt
    
    # bin
    
    # Parquet
    
    # HDF5
    
    # Feather
    
    # JSON
    
    # AVRO
    
    # ORC


if __name__ == "__main__":
    main()
