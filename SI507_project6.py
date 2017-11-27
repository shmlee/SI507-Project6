# Import statements
import psycopg2
import psycopg2.extras
from config import *
import csv


# Write code / functions to set up database connection and cursor here.
db_connection, db_cursor = None, None #global 
def get_connection_and_cursor():
    global db_connection, db_cursor
    if not db_connection:
        try:
            if db_password != "":
                db_connection = psycopg2.connect("dbname='{0}' user='{1}' password='{2}'".format(db_name, db_user, db_password))
                print("Success connecting to database")
            else:
                db_connection = psycopg2.connect("dbname='{0}' user='{1}'".format(db_name, db_user))
        except:
            print("Unable to connect to the database. Check server and credentials.")
            sys.exit(1) # Stop running program if there's no db connection.

    if not db_cursor:
        db_cursor = db_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    return db_connection, db_cursor

db_connection, db_cursor = get_connection_and_cursor()


#  Write code / functions to create tables with the columns you want and all database setup here.
def setup_database():

    conn, cur = get_connection_and_cursor()
    db_cursor.execute("DROP TABLE IF EXISTS Sites")
    db_cursor.execute("DROP TABLE IF EXISTS States")

    db_cursor.execute("CREATE TABLE IF NOT EXISTS States(ID SERIAL PRIMARY KEY, NAME VARCHAR(40) UNIQUE)")
    db_cursor.execute("CREATE TABLE IF NOT EXISTS Sites(ID SERIAL PRIMARY KEY, NAME VARCHAR(128) UNIQUE, TYPE VARCHAR(128), State_ID INTEGER REFERENCES States(ID), Location VARCHAR(255), Description TEXT)")

    db_connection.commit()

setup_database()
 
  

# Write code / functions to deal with CSV files and insert data into the database here.

def insert(files, each_state): #got help from Kenji
    conn, cur = db_connection, db_cursor

    from csv import DictReader
    read_dict = DictReader(open(files, 'r')) #converts to dic

    cur.execute("INSERT INTO States(Name) VALUES (%s) RETURNING ID", (each_state,))
    result = cur.fetchone()
    # print(result)

    for each_dict in read_dict:
        cur.execute("""INSERT INTO Sites(Name, Type, State_Id, Location, Description)
            VALUES (%s, %s, %s, %s, %s)""",
            (each_dict['NAME'], each_dict['TYPE'], result['id'], each_dict['LOCATION'], each_dict['DESCRIPTION']))



# Make sure to commit your database changes with .commit() on the database connection.
db_connection.commit()


# Write code to be 'invoked here (e.g. invoking any functions you wrote above)
insert('arkansas.csv', 'Arkansas')
insert('california.csv', 'California')
insert('michigan.csv', 'Michgan')


def execute(query):
    conn, cur = db_connection, db_cursor
    cur.execute(query)
    results = cur.fetchall()
    for r in results:
        print(r)
    print('--> Result Rows:', len(results))
    print()
   

# Write code to make queries and save data in variables here.
db_cursor.execute('SELECT location from sites')
all_locations = db_cursor.fetchall()
print(all_locations)

db_cursor.execute("""SELECT name FROM sites WHERE description ilike '%beautiful%'""")
beautiful_sites = db_cursor.fetchall()
print(beautiful_sites)

db_cursor.execute("""SELECT Count (*) FROM sites WHERE type = 'National Lakeshore' """)
natl_lakeshores = db_cursor.fetchall()
print(natl_lakeshores)

db_cursor.execute("""SELECT "sites"."name" FROM "sites" INNER JOIN "states" ON ("sites"."state_id") = ("states"."id") WHERE ("sites"."state_id") = 3 """ )
michigan_names = db_cursor.fetchall()
print(michigan_names)

db_cursor.execute("""SELECT Count (*) FROM "sites" INNER JOIN "states" ON ("sites"."state_id") = ("states"."id") WHERE ("sites"."state_id") = 1 """)
total_number_arkansas = db_cursor.fetchall()
print(total_number_arkansas)


