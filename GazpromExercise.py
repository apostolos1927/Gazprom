import json, psycopg2, sys, csv
from psycopg2 import connect, Error
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


#Connection to the database using psycopg2
connection = psycopg2.connect(user="postgres",
                                  password="....",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="mydb")
       
       


try:
   cursor = connection.cursor()
   ##CONSU and HEADR data will be separated and stored in two different tables in the database as they have different number of columns and are different type of data
   ##Below there are two tables: CONSUDATA where we stored the body of the dataset and HEADERDATA where we store the header data. 
   ##We add there columns: Received_at to store when the file was loaded/received, Filename which stores the filenames, Common_id is a unique id we generated which will be common for both tables
   sqlquery1 = "create table if not exists CONSUDATA (Recordtype varchar(80),Meter_Number varchar(80), Measurement_date date, Measurement_time time,Consumption real DEFAULT NULL,Received_at TIMESTAMP, Filename varchar(80),Common_id varchar(150));"
   
   cursor.execute(sqlquery1)
   
   sqlquery2 = "create table if not exists HEADERDATA(Recordtype varchar(80),Filetype varchar(80), Company_id varchar(80), Creation_date date,Creation_time time, Gen_num varchar(80),Received_at TIMESTAMP, Filename varchar(80),Common_id varchar(150));"
   
   cursor.execute(sqlquery2)
   
   
   
   #path to load the data
   path = r'C:/Users/..../DE_20191129/' 
   #we load all the SMRT files
   all_files = glob.glob(path + "*.SMRT")

   #we iterate through all files we load
   for filename in all_files:
     #getting the head and tail data to get filenames
     head, tail = ntpath.split(filename)
     #we query the data to see if the filename was previously loaded
     sql_select_query = """SELECT * FROM HEADERDATA WHERE filename = %s limit 1"""
     cursor.execute(sql_select_query, (tail,))
     count = cursor.rowcount
     #if the result is different than 0 it means we have loaded this file before so we skip this iteration
     if count!=0:
      continue
     #Creating a unique id so both tables have a key field in common
     Common_id=str(uuid.uuid1())
     #we load the file into a pandas dataframe
     df = pd.read_csv(filename, index_col=None, header=None)
     
     #if the first row does not have header we don't load the file
     l=df.head(1).iloc[:,0].str.find("HEADR")
     if int(l) ==-1:  
        continue
     #if the last line does not have trail we don't load the file
     elif int(df.tail(1).iloc[:,0].str.find("TRAIL")) == -1:
        continue
     else:
        #initialize a dictionary where we store the last updated value of datetime of any given meter in key value pairs
        dicty={}
        #we iterate through the pandas dataframe 
        for j, inner in df.iloc[1:].iterrows():
           #if this particular key value pair does not exist in the dictionary we load it 
           if not dicty:
            dicty={inner[1]:inner[2]}
            #else we overwrite the original value with a new one for any given meter
           else:
            dict2={inner[1]:inner[2]}
            dicty.update(dict2)
        
        #iterate pandas framework again to store the data this time
        for i, row in df.iterrows():
         #if it's the header we store the data into the HEADERDATA table
         if row[0]=="HEADR":
          #if time is 0 as an integer then we convert it to '0000'
          if(int(row[4])==0):
           a = pd.to_datetime("0000", format='%H%M%S')
          else:
           #we get the integer part of the number and convert it to the right time format (HHMMSS)
           int_part=(re.search(r'\d+', str(row[4])).group())
           a = pd.to_datetime(int_part, format='%H%M%S')
          #we store the data
          cursor.execute("INSERT INTO HEADERDATA VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s)", (row[0], row[1], row[2],datetime.datetime.strptime(str(int(row[3])), '%Y%m%d'),str(a.hour)+":"+str(a.minute)+":"+str(a.second),row[5],datetime.datetime.now() ,tail,Common_id))
         #if it's the body of the dataset
         elif row[0]=="CONSU":  
          #we convert the time to the right format
          if(int(row[3])==0):
           a = pd.to_datetime("0000", format='%H%M%S')
          else:
           int_part=(re.search(r'\d+', str(row[3])).group())
           a = pd.to_datetime(int_part, format='%H%M%S')
          #store the data to the CONSUDATA table
          cursor.execute("INSERT INTO CONSUDATA VALUES (%s,%s, %s, %s, %s, %s, %s, %s)", (row[0], row[1], datetime.datetime.strptime(str(int(dicty[row[1]])), '%Y%m%d'),str(a.hour)+":"+str(a.minute)+":"+str(a.second),row[4],datetime.datetime.now() ,tail,Common_id)) 
         else:
          #if it's TAIL or something else we skip the row
          print("Not proper data to store into the tables")        
         
     
   #close connection
   connection.commit()
   print ("Record inserted successfully into table")
#for any exceptions when we perform queries
except (Exception, psycopg2.Error) as error :
    if(connection):
        print("Failed to insert record into table", error)
#finally to make sure we close the cursor and the connection no matter what.
finally:   
    if(connection):
        cursor.close()
        connection.close()
        print("PostgreSQL connection is closed")



