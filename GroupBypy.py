import pandas as pd
import numpy as np
import chardet
import os

def find_encoding(fname):
    r_file = open(fname, 'rb').read()
    result = chardet.detect(r_file)
    charenc = result['encoding']
    return charenc

if __name__ == '__main__':
    
    for a in os.listdir("CSV/"):
        print(a)
        my_encoding = find_encoding("CSV/"+a )
        df = pd.read_csv("CSV/"+a , encoding=my_encoding)
        print(df)
        dfn = df.groupby(['Texto','Mes', "Ciudad", "País"]).count()
        print(dfn)
        dfn.to_csv("CSVFInales/"+ a +"-2.csv", sep=",", encoding="utf8")