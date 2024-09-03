import pandas as pd
import re
import warnings
import datetime
from assets.config.config_ingestion import logger
warnings.simplefilter(action='ignore', category=FutureWarning)
warnings.simplefilter(action='ignore', category=UserWarning)


class Saneamento:
    '''
    Classe de saneamento
    '''
    
    def __init__(self, data, metadados):
        self.data = data
        self.metadados = metadados

    def select_rename(self):
        self.data = self.data.loc[:, self.metadados["cols_originais"]] 
        self.data.rename(columns=self.metadados["cols_renamed"], inplace = True)

    def null_exclude(self):
        for col in self.metadados["cols_chaves"]:
            self.data = self.data.loc[~self.data[col].isna()]
        for col in self.metadados["fillna"]:
            self.data = self.data.fillna(self.metadados["fillna"][col])

    def convert_data_type(self):
        for col in self.metadados["tipos"].keys():
            tipo = self.metadados["tipos"][col]
            if tipo == "int":
                tipo = self.data[col].astype(int)
            elif tipo == "float":
                self.data[col] = self.data[col].replace(",", ".")
                self.data[col] = self.data[col].astype(float)
            elif tipo == "datetime":
                self.data[col] = pd.to_datetime(self.data[col])
            elif tipo == "string":
                self.data[col] = self.data[col].astype(str)
                self.data[col] = self.data[col].str.replace('.0', '')

    def string_std(self):
        for col in self.metadados["std_str"]:
            new_col = f'{col}_formatted'
            self.data[new_col] = self.data.loc[:,col].apply(
                lambda x: re.sub('[^A-Za-z0-9]+', '', x.upper())
                )
            
    def null_check(self):
        for col in self.metadados["null_tolerance"].keys():
            col_nuls = len(self.data.loc[self.data[col].isnull()])
            if  col_nuls/len(self.data) > self.metadados["null_tolerance"][col]:
                logger.warning(
                    f"{col} possui mais nulos do que o esperado; {datetime.datetime.now()}")
            else:
                logger.info(
                    f"{col} possui nulos dentro do esperado; {datetime.datetime.now()}") 
    
    def fetch_df(self):
        return self.data



def corrige_hora(hr_str, dct_hora = {1:"000?",2:"00?",3:"0?",4:"?"}):
    if hr_str == "2400":
        return "00:00"
    elif (len(hr_str) == 2) & (int(hr_str) <= 12):
        return f"0{hr_str[0]}:{hr_str[1]}0"
    else:
        hora = dct_hora[len(hr_str)].replace("?", hr_str)
        return f"{hora[:2]}:{hora[2:]}"