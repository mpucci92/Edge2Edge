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
                            'Aftercooler Temperature': 'Uranium Production'},errors="raise")
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
    send_interval = 2

    columns_to_keep = ['Haul Truck Template', 'Aftercooler Temperature']

    truck_status = ['Running', 'Running', 'Running', 'Running', 'Running', 'Running',
                    'Running', 'Partial Running']

    df = loadData(path_to_data, encoding, delimiter)
    df = df.loc[:, columns_to_keep]
    df = renameColumns(df)
    df = df[df['Uranium Mill'] == "Truck 101"] # 1 Asset Specification
    df['Uranium Mill'] = np.where(df['Uranium Mill'] == "Truck 101", "Mill 1", df['Uranium Mill'])


    while True:
        list_of_trucks = df['Uranium Mill'].value_counts().index.tolist()
        list_of_properties = list(df.columns)[1:]

        for asset in list_of_trucks:  # truck01
            dictionary = {}
            for prop in list_of_properties:
                topic = topic_constant + topic_delimiter + asset
                dictionary[prop] = np.random.choice(df[df['Uranium Mill'] == asset][prop])

            try:
                publish.single(topic, str(dictionary), hostname=hostname_broker)
            except Exception as e:
                print(topic)

            time.sleep(send_interval)
