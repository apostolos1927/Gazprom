import json, psycopg2, sys, csv
#from smart_open import open
from psycopg2 import connect, Error
#import time;
import datetime;
from pathlib import Path
import os
import ntpath
import uuid
import numpy as np 
import datetime
import pandas as pd
import glob
import re
connection = psycopg2.connect(user="postgres",
                                  password="Enfield7*",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="mydb")
       
       

#creating a connection to my postgresql database 
try:
   cursor = connection.cursor()
   sqlCreateSource = "create table if not exists DataTest54 (Recordtype varchar(80),Meter_Number varchar(80), Measurement_date date, Measurement_time time,Consumption real DEFAULT NULL,Received_at TIMESTAMP, Filename varchar(80),Common_id varchar(150));"
   
   cursor.execute(sqlCreateSource)
   
   sqlCreateSource2 = "create table if not exists HeaderTest54(Recordtype varchar(80),Filetype varchar(80), Company_id varchar(80), Creation_date date,Creation_time time, Gen_num varchar(80),Received_at TIMESTAMP, Filename varchar(80),Common_id varchar(150));"
   
   cursor.execute(sqlCreateSource2)
   
   
   
   
   path = r'C:/Users/apost/Documents/Assignment/DE_20191129/sample_data/' # use your path
   all_files = glob.glob(path + "*.SMRT")

  
   for filename in all_files:
     #print(filename)
     head, tail = ntpath.split(filename)
     print(tail)
     sql_select_query = """SELECT * FROM HeaderTest54 WHERE filename = %s limit 1"""
     
     
     cursor.execute(sql_select_query, (tail,))
     count = cursor.rowcount
     if count!=0:
      continue
     Common_id=str(uuid.uuid1())
     df = pd.read_csv(filename, index_col=None, header=None)
     
     #print("df",df)
     #print(df.head(1))
     #print(df.tail(1))
     l=df.head(1).iloc[:,0].str.find("HEADR")
     print("lllll",l)
     if int(l) ==-1:
       
        continue
     elif int(df.tail(1).iloc[:,0].str.find("TRAIL")) == -1:
        print("222222222222")
        continue
     else:
        dicty={}
        for j, inner in df.iloc[1:].iterrows():
           if not dicty:
            dicty={inner[1]:inner[2]}
           else:
            print("inner",inner[1])
            print("inner",inner[2])
            dict2={inner[1]:inner[2]}
            print("dict2",dict2)
            dicty.update(dict2)
        print("dict is ",dicty)
        for i, row in df.iterrows():
        
         if row[0]=="HEADR":
          print("test3")
          
          
          if(int(row[4])==0):
           a = pd.to_datetime("0000", format='%H%M%S')
          else:
           int_part=(re.search(r'\d+', str(row[4])).group())
           a = pd.to_datetime(int_part, format='%H%M%S')
          print("test4")
          cursor.execute("INSERT INTO HeaderTest54 VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s)", (row[0], row[1], row[2],datetime.datetime.strptime(str(int(row[3])), '%Y%m%d'),str(a.hour)+":"+str(a.minute)+":"+str(a.second),row[5],datetime.datetime.now() ,tail,Common_id))
         elif row[0]=="CONSU":  
          
          
          if(int(row[3])==0):
           a = pd.to_datetime("0000", format='%H%M%S')
          else:
           int_part=(re.search(r'\d+', str(row[3])).group())
           a = pd.to_datetime(int_part, format='%H%M%S')
          print("test5",dicty[row[1]])
          cursor.execute("INSERT INTO DataTest54 VALUES (%s,%s, %s, %s, %s, %s, %s, %s)", (row[0], row[1], datetime.datetime.strptime(str(int(dicty[row[1]])), '%Y%m%d'),str(a.hour)+":"+str(a.minute)+":"+str(a.second),row[4],datetime.datetime.now() ,tail,Common_id)) 
         else:
          print("ERROR")        
         
     
   #close connection
   connection.commit()
   print ("Record inserted successfully into table")

except (Exception, psycopg2.Error) as error :
    if(connection):
        print("Failed to insert record into table", error)

finally:
    
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")



