import sys
from datetime import date
from jugaad_data.nse import bhavcopy_save, bhavcopy_fo_save
import pandas as pd

def generate_dataframe(to_date, symbol):
    from jugaad_data.nse import stock_df
    df = pd.DataFrame(stock_df(symbol=symbol, from_date=date(to_date,1,16), to_date=date(2023,1,16), series="EQ"))
    df.to_csv(path_or_buf = symbol + "_sol.csv" )
    df = df[[ "DATE", "OPEN", "CLOSE", "HIGH","LOW", "LTP", "VOLUME", "VALUE","NO OF TRADES"]]
    return df

def main():
    to_date = 2023-int(sys.argv[2])
    symbol = sys.argv[1]

    DATA = generate_dataframe(to_date, symbol)
    print(DATA)



if __name__ == "__main__":
    main()