import pandas as pd
import numpy as np
import time
import os
from datetime import datetime
import paho.mqtt.publish as publish

def loadData(path, encoding, delimiter):
    df = pd.read_csv(path, encoding=encoding, delimiter=delimiter)
    return df


def renameColumns(df):
    df = df.rename(columns={'Haul Truck Template':'Uranium Mill',
                            'Aftercooler Temperature': 'UraniumProduction'},errors="raise")
    return df

# Directory Settings #
directory = (os.path.dirname(os.path.realpath(__file__)))
data_directory = '\\Data\\'

if __name__ == "__main__":

    # Input Parameters #
    path_to_data = directory + data_directory + 'FleetDataOCS.csv'
    encoding = 'utf-8'
    delimiter = ';'
    topic_delimiter = "/"
    topic_constant = 'fleet'
    hostname_broker = "localhost"
    send_interval = 1

    containerID = "MyCustomContainer"

    columns_to_keep = ['Haul Truck Template', 'Aftercooler Temperature']

    df = loadData(path_to_data, encoding, delimiter)
    df = df.loc[:, columns_to_keep]
    df = renameColumns(df)
    df = df[df['Uranium Mill'] == "Truck 101"] # 1 Asset Specification
    df['Uranium Mill'] = np.where(df['Uranium Mill'] == "Truck 101", "Mill 1", df['Uranium Mill'])

    print(datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ"))
    data_dictionary = {}
    values = []
    finalData = []
    data_dictionary["containerid"] = containerID

    while True:
        dictTemp = {}

        dictTemp['Timestamp'] = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
        dictTemp['Value'] = np.random.choice(df[df['Uranium Mill'] == 'Mill 1']['UraniumProduction'])
        values.append(dictTemp)
        data_dictionary['values'] = values

        finalData = [data_dictionary]
        finalData = (list(finalData))

        if len(data_dictionary['values']) == 25:
            data_dictionary['values'].pop(0)

        print(finalData)
        print(len(data_dictionary['values']))
        time.sleep(send_interval)
