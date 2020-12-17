from flask import Flask
from flask_jsonpify import jsonify
import json, psycopg2, sys, csv
#from smart_open import open
from psycopg2 import connect, Error

app = Flask(__name__)

connection = psycopg2.connect(user="postgres",
                                  password="Enfield7*",
                                  host="127.0.0.1",
                                  port="5432",
                                  database="mydb")

@app.route("/")
def index():
        cursor = connection.cursor()
        sql_select_query = """SELECT DISTINCT Meter_Number FROM DataTest54"""
        cursor.execute(sql_select_query)
        result=cursor.fetchall()
        connection.commit()
        cursor.close()
        connection.close()
        return jsonify(result)
        
        
        
@app.route("/numfiles")
def num_files():
        cursor = connection.cursor()
        sql_select_query = """SELECT DISTINCT Filename FROM DataTest54"""
        cursor.execute(sql_select_query)
        result=cursor.fetchall()
        count = cursor.rowcount
        cursor.close()
        connection.commit()
        connection.close()
        
        return str(count)       

@app.route("/lastfile")
def lastfile():
        cursor = connection.cursor()
        sql_select_query = """SELECT Filename,received_at FROM DataTest54 GROUP BY Filename,received_at ORDER BY received_at DESC limit 1 """
        cursor.execute(sql_select_query)
        result=str(cursor.fetchall())
        start = "'"
        end = "'"
        Substring=result[result.find(start)+len(start):result.rfind(end)]
        cursor.close()
        connection.commit()
        connection.close()
        
        return str(Substring)          

@app.route("/<string:mnum>")
def sec_q(mnum):
        print("id",mnum)
        cursor = connection.cursor()
        sql_select_query = """SELECT * FROM DataTest54 where Meter_Number = %s"""
        cursor.execute(sql_select_query, (mnum,))
        result=cursor.fetchall()
        json_data=[]
        for r in result:
         json_data.append(r)
        connection.commit()
        cursor.close()
        connection.close()
        return str(json_data)

if __name__ == "__main__":
    app.run()

