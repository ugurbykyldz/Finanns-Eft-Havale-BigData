from datetime import datetime , timedelta
import random

class DataGenerator:
    
    def __init__(self):

        
        self.pid = 10000

        with open("sehir.txt" , "r", encoding="utf-8") as f :
            data_city = f.readlines()
            self.city = [  i.split("\n")[0] for i in data_city]
            
        with open("isimler.txt", "r", encoding = "utf-8") as f:
            data_name = f.readlines()
            self.name = [ i.split("\n")[0]  for i in data_name] 
            
        with open("soyisimler.txt", "r", encoding = "utf-8") as f:
            data_surname = f.readlines()
            self.surname = [ i.split("\n")[0]  for i in data_surname]    
            
                       
        
    def generatorDATA(self):
        
        account = {
            "oid" : self.generateID(),
            "name" : self.generateNameSurname(),
            "iban" : self.generateIbanNumber()
        }
        info = {
                "name" : self.generateNameSurname(),
                "iban" : self.generateIbanNumber(),
                     

        }

        self.pid += 1
        data = {
            "pid": self.pid,
            "pytpe": self.generatePTYPE(),
            "account": account,
            "info" : info,
            "balance": self.generatorBalance(),
            "btype" : self.generateCurrency(),
            "device" : self.generateDevice(),
            "city" : self.generaterCity(),
            "bank" : self.generateBank(), 

        }

        return data
    
    def generateNameSurname(self):
        return self.name[random.randint(0,len(self.name)-1)] +" "+ self.surname[random.randint(0, len(self.surname)-2)]
    
    def generateIbanNumber(self):
        return "TR"+str(random.randint(0,999999999999)+1000000000000)

    
    def generateID(self):
        number = random.randint(0,len(self.surname) * len(self.name)-1)
        return number  
    
    
    def generateBank(self):
        bank = ["World Bank", "Happy Bank", "Water Bank", "Love Bank"]
    
        return bank[random.randint(0, len(bank)-1)]
    
    def generateDevice(self):
        device = ["Web aplication", "Mobile aplication"]
        return device[random.randint(0, 1)]
    
    def generateCurrency(self):
        currency = ["TL", "USD", "EURO"]
        return currency[random.randint(0, len(currency)-1)]
    
    def generatePTYPE(self):
        eft = ["H", "E"]
        return eft[random.randint(0,1)]
    
    def generatorBalance(self):
        return random.randint(50, 1500)
    
    def generaterCity(self):
        return self.city[random.randint(0, len(self.city)-1)]

