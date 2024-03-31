import schedule as sc
import time as t
from enum import Enum
from datetime import datetime, timedelta
import time
from random_forest_implementation import Random_Forest_Model
import random
import pprint
import json
import numpy as np
from pushDataToDB import pushDataToSQL

number_of_days = 1

class Fruits_Vegies(Enum):
    """
    The following enum stores all the fruits and vegies information
    """
    Apple = (0, {"Temp": (30, 351), "CO2": (300, 450), "Humidity": (90, 901), "Sent": 40, "Received": 40})
    Banana = (1, {"Temp": (320, 400), "CO2": (300, 450), "Humidity": (953, 953), "Sent": 40, "Received": 40})
    Grapes = (2, {"Temp": (30, 32), "CO2": (300, 450), "Humidity": (90, 95), "Sent": 40, "Received": 40})
    Lemons = (3, {"Temp": (45, 50), "CO2": (300, 450), "Humidity": (85, 90), "Sent": 40, "Received": 40})
    Mangoes = (4, {"Temp": (50, 55), "CO2": (300, 450), "Humidity": (85, 90), "Sent": 40, "Received": 40})
    Tomatoes = (5, {"Temp": (55, 85), "CO2": (300, 450), "Humidity": (60, 85), "Sent": 40, "Received": 40})
 
    def get_value(self):
        """
        The following method is used to get the name of the fruit/vegie
        """
        return self.value[0]
   
    def get_info(self):
        """
        The following method is used to get the information related to fruit/vegie
        """
        return self.value[1]
   
class Transit_Information(Enum):
    """Transit ID, Region ID, Region Name, Start, Destination"""
    Transit1 = (1, 1, "CA", "BritishColumbia", "NovaScotia")
    Transit2 = (2, 1, "CA", "NewBrunswick", "Alberta")
    Transit3 = (3, 1, "CA", "Newfoundland", "BritishColumbia")
    Transit4 = (4, 1, "CA", "NewBrunswick", "NovaScotia")
    Transit5 = (5, 1, "CA", "BritishColumbia", "Alberta")
    Transit6 = (6, 1, "CA", "NovaScotia", "Alberta")
 
class Data_Generator():
    """
    This class is used to generate data that will be later used to predict and send to the database.
    """
    def __init__(self) -> None:
        self.__data = dict()  
        self.__random_forest_model = Random_Forest_Model()  
        self.number_of_hours = 0  
        self.__co2 = 0
        self.__temp = 0
        self.__humidity = 0
        self.__did_receive = False
   
    def create_base_data(self) -> list:
        """
        We wil be using this function to generate data
        This is supposed to be data taken from IoT sensors but in this case we are mocking it.
       
        Returns:
            list - A list of predicted records in the form of dictionary.
        """
        __predicted_record = list()
       
        for transit in Transit_Information:
            data = dict()
            for fruit_vegie in Fruits_Vegies:
                # print(f"\n\n\n\n{fruit_vegie.get_value()}\n\n\n\n\n")
                fruit_data = {}
                info = fruit_vegie.get_info()
                time_str = time.strftime("%Y-%m-%d %H:%M:%S")
               
                fruit_data[time_str] = {
                    "Temp": self.__temp,
                    "Humidity": self.__humidity,
                    "CO2": self.__co2,
                    "Days": number_of_days,
                    "Max Temp": info["Temp"][1],
                    "Min Temp": info["Temp"][0],
                    "Max Humidity": info["Humidity"][1],
                    "Min Humidity": info["Humidity"][0],
                    "Max Co2 ": info["CO2"][1],
                    "Min Co2 ": info["CO2"][0],
                }
                data[fruit_vegie.get_value()] = fruit_data
                self.__data = self.__random_forest_model.get_predicted_record_set({fruit_vegie.get_value(): data[fruit_vegie.get_value()]})
                self.__data['Fruit ID'] = fruit_vegie.get_value()+1
                self.__data['Transit ID'] = transit.value[0]
                self.__data['Region ID'] = transit.value[1]
                self.__data['Region Name'] = transit.value[2]
                self.__data['Start'] = transit.value[3]
                self.__data['Destination'] = transit.value[4]
                if self.__did_receive == False:
                    self.__data['Sent'] = info["Sent"]
                else:
                    self.__data['Sent'] = info["Sent"]
                    self.__data['Received'] = info["Received"]
                __predicted_record.append(self.__data)
 
        # Add your code here to send `self.__predicted_record` or use the returned list.
        pp = pprint.PrettyPrinter(indent=4)
        # for i in __predicted_record:
        #     pp.pprint(i)
        return pushDataToSQL(__predicted_record)

    def generate_random_values(self):
        """
        This function is used to generate random values
        """
        if self.number_of_hours >= 4:
            self.number_of_hours = 0
            self.__temp = random.uniform(30, 400 + 10)
            self.__co2 = random.uniform(300 - 10, 450 + 10)
            self.__humidity = random.uniform(85 - 10, 100 + 10)
            self.__did_receive = True
            return
        if self.number_of_hours == 0:
            self.number_of_hours += 1
            self.__temp = random.uniform(30, 400 + 10)
            self.__co2 = random.uniform(300 - 10, 450 + 10)
            self.__humidity = random.uniform(85 - 10, 100 + 10)
            # self.__did_receive = False
        elif self.number_of_hours <=4:
            self.number_of_hours += 1
            self.__did_receive = False
       
 
    def check_days_passed(self):
        """
        This function is used to increment number of days every 24 hours
        """
        global number_of_days
        number_of_days += 1
        # print(f"days passed: {number_of_days}")
 
    def start(self):
        sc.every(1).seconds.do(self.generate_random_values)
        sc.every(1).seconds.do(self.create_base_data)
        sc.every(24).minutes.do(self.check_days_passed)
        while True:
            sc.run_pending()
            time.sleep(1)

    # To keep this script running add
        # while True:
            # schedule.run_pending()
            # time.sleep(1)
 
if __name__ == "__main__":
    dg = Data_Generator()
    dg.start()