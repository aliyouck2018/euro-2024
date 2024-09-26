import pandas as pd
import mysql.connector
from datetime import datetime

print('Loading the csv file...')
players = pd.read_csv('euro2024.csv',sep=',')

DB_NAME = 'kaggle'
TABLES = {}
TABLES['players'] = (
    "CREATE TABLE `players` ("
    "  `Name` varchar(40),"
    "  `Position` varchar(40),"
    "  `Age` varchar(40) ,"
    "  `Club` varchar(40) ,"
    "  `Height` varchar(40) ,"
    "  `Foot` varchar(40) ,"
    "  `Caps` varchar(40) ,"
    "  `Goals` varchar(40) ,"
    "  `MarketValue` varchar(40) ,"
    "  `Country` varchar(40) "
    ") ENGINE=InnoDB")

connection = mysql.connector.connect(user='root',password='fanell')
cursor = connection.cursor()

def create_database(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

    try:
        cursor.execute("USE {}".format(DB_NAME))
    except mysql.connector.Error as err:
        print("database {} does not exists.".format(DB_NAME))
        if err.errno == errorcode.ER_BAD_DB_ERROR:
            create_database(cursor)
            print("database {} created successfully.".format(DB_NAME))
            cnx.database = DB_NAME
        else:
            print(err)
            exit(1)


def create_table(cursor):
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            print("Creating table {}: ".format(table_name), end='')
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                print("already exists.")
            else:
                print(err.msg)
        else:
            print("OK")


def insert_data(cursor):
	print('Inserting data ...')
	for index, player in players.iterrows():
	    add_player = ("INSERT INTO transactions "
	       "(Name,Position,Age,Club,Height,Foot,Caps,Goals,MarketValue,Country) "
	       "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)")

	    player_ = (
	        player['Name'],
	        player['Position'],
	        player['Age'],
	        player['Club'],
	        player['Height'],
	        player['Foot'],
	        player['Caps'],
	        player['Goals'],
	        player['MarketValue'],
	        player['Country'])

	    cursor.execute(add_player, player_)  

create_database()
create_table(cursor)
insert_data(cursor)

connection.commit()
cursor.close()
connection.close()