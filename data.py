""" Dnas ce code nous allons interagir avec le coinapi et une base de donnée
SQLITE, nous pourrons faire appel a l'api grace a la clé secrete de l'api 
stocker dans le fichier .env et appelé dans le config
"""


from config import settings 
import requests
import pandas as pd
from datetime import date, timedelta
import sqlite3



class COINAPI:
    def __init__(self, api_key=settings.coin_api_key) -> None:
        self.__api_key = api_key
        
    def get_daily(self, asset, output_size=100):
        """ get daily time series from Coinapi

        Parameters
        ---------
        asset : str
            the ticker symbol of the asset
        output_size : int
            Number of the observations to retrieve
            maximun is 100
            
        returns
        -------
        pd.DataFrame
         columns are 'open', 'high', 'low', 'close'.
         all numeric
        """
        start_time = (date.today() - timedelta(50)).strftime("%Y-%m-%d")
        end_time = date.today().strftime("%Y-%m-%d")
        
        # Create URL
        url = ("https://rest.coinapi.io/v1/"
            f"exchangerate/{asset}/"
            "history?period_id=1DAY&"
            f"time_start={start_time}T00:00:00&"
            f"time_end={end_time}T00:00:00")
        
        headers = {'X-CoinAPI-Key' : f'{self.__api_key}'}
        
        # send call to  api
        response = requests.get(url, headers)
        # extract json data
        json_data = response.json()
        # read data into dataframe
        df = pd.DataFrame(response)
        #select important columns
        columns = ["time_close", "rate_open", "rate_high", "rate_low", "rate_close"]
        df = df[columns]
        #create new index convert it into "DatetimeIndex" and named "date" 
        df = df.set_index("time_close")
        df.index = pd.to_datetime(df.index)
        df.index.name = "date"
        
        # remove "_rate" into columns
        df.columns = [c.split("_")[-1] for c in df.columns]

        #return DataFrme
        
        return df
        


    
class SQLRepository:
    def __init__(self, connection) -> None:
        self.connection = connection
        
    def insert_table(self, table_name):
        pass
    
    def read_table(self, table_name, limit=None):
        pass