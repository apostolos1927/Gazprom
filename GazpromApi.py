from flask import Flask
from flask_jsonpify import jsonify
import json, psycopg2, sys, csv
from psycopg2 import connect, Error


#Flash instance creation
app = Flask(__name__)

#connect to postgresql database
connection = psycopg2.connect(user="postgres",
                                  password="....",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="mydb")

#Question 1 - returns all the unique meters of the dataset
@app.route("/")
def index():
        cursor = connection.cursor()
        sql_select_query = """SELECT DISTINCT Meter_Number FROM CONSUDATA"""
        cursor.execute(sql_select_query)
        result=cursor.fetchall()
        connection.commit()
        cursor.close()
        connection.close()
        return jsonify(result)
        
#Question 2 - returns the data for a given meter      
@app.route("/<string:mnum>")
def sec_q(mnum):
       
        cursor = connection.cursor()
        sql_select_query = """SELECT * FROM CONSUDATA where Meter_Number = %s"""
        cursor.execute(sql_select_query, (mnum,))
        result=cursor.fetchall()
        #populate a dictionary with the result of fetchall() and return a 
        json_data=[]
        for r in result:
         json_data.append(r)
        connection.commit()
        cursor.close()
        connection.close()
        return str(json_data)        

#Question 3 - returns the number of files we received
@app.route("/numfiles")
def num_files():
        cursor = connection.cursor()
        sql_select_query = """SELECT DISTINCT Filename FROM CONSUDATA"""
        cursor.execute(sql_select_query)
        result=cursor.fetchall()
        count = cursor.rowcount
        cursor.close()
        connection.commit()
        connection.close()
        
        return str(count)       


#Question 4 - returns the last file we received
@app.route("/lastfile")
def lastfile():
        cursor = connection.cursor()
        sql_select_query = """SELECT Filename,received_at FROM CONSUDATA GROUP BY Filename,received_at ORDER BY received_at DESC limit 1 """
        cursor.execute(sql_select_query)
        result=str(cursor.fetchall())
        start = "'"
        end = "'"
        Substring=result[result.find(start)+len(start):result.rfind(end)]
        cursor.close()
        connection.commit()
        connection.close()
        
        return str(Substring)          
#

if __name__ == "__main__":
    app.run()

