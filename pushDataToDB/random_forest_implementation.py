import pandas as pd
from joblib import load

class Random_Forest_Model():
    """
    This class is an implementation of random forest that provides prediction for a given record.
    """

    def __init__(self) -> None:
        # Trained random forest model checkpoint will be loaded into classifier
        self.classifier = load('random_forest.joblib')
        self.datetime = ""
        self.fruits = {0:"Apple", 1:"Banana", 2:"Grapes", 3:"Lemons", 4:"Mangoes", 5:"Tomatoes"}

    def __get_dataframe_using(self, dictionary: dict) -> pd.DataFrame:
        """
        We convert the dictionary to dataframe here.
        """
        df = pd.concat({k: pd.DataFrame(v).T for k, v in dictionary.items()}, axis=0).reset_index()
        df.columns = ['Fruit', 'Datetime', "Temp","Humidity", "CO2", "Days", "Max Temp", "Min Temp", "Max Humidity", "Min Humidity", "Max Co2", "Min Co2",]
        self.datetime = df['Datetime'][0]
        df = df.drop('Datetime', axis = 1)
        return df

    def __convert_to_dictionary(self, df: pd.DataFrame, pred: int) -> dict:
        """
        Post prediction we return the dictionary that can be used to store into the database.
        """
        fruit = self.fruits[df['Fruit'][0]]
        data = {
                self.datetime : {
                    "Temp": df['Temp'][0],
                    "CO2": df['CO2'][0],
                    "Humidity": df['Humidity'][0],
                    "Days": df['Days'][0],
                    
                    "Spoiled": pred
                },
                "Max Temp": df['Max Temp'][0],
                "Min Temp": df['Max Temp'][0],
                "Max Co2": df['Max Co2'][0],
                "Min Co2": df['Min Co2'][0],
                "Max Humidity": df['Max Humidity'][0],
                "Min Humidity": df['Min Humidity'][0]
            }
        return {
            fruit : data
        }

    def get_predicted_record_set(self, dictionary: dict) -> dict:
        """
        Given a record, we convert it to dataframe, get prediction and return the dicitonary for that predicted record.
        """
        df = self.__get_dataframe_using(dictionary=dictionary)
        pred = self.classifier.predict(df)[0]
        return self.__convert_to_dictionary(df=df, pred=pred)
    
if __name__ == "__main__":
    df = { "Apple": {
            "12" : {
                "Temp": "Temp", 
                "CO2": "co2",
                "Humidity": "humidity", 
                "Max Temp": "info[][1]",
                "Min Temp": "info[][0]",
                "Max CO2": "info[][1]",
                "Min CO2": "info[][0]",
                "Max Humidity": "info[]][1]",
                "Min Humidity": "info[][0]",
                "Days": "number_of_days"
            }
        }
    }
    Random_Forest_Model().get_predicted_record_set(df)