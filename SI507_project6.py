# Import statements
import psycopg2
import sys
import psycopg2.extras
import csv
from psycopg2 import sql
from config_example import *

# Write code / functions to set up database connection and cursor here.

def get_connection_and_cursor():
    try:
        db_conn = psycopg2.connect("dbname = '{0}' user = '{1}' password = '{2}'".format(dbname, username, password))
        print("connected")

    except:
        print("fail to connect")
        sys.exit(1)

    db_cursor = db_conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    return db_conn, db_cursor


# Write code / functions to create tables with the columns you want and all database setup here.

def set_up_db():
    cur.execute("DROP TABLE IF EXISTS Sites")
    cur.execute("DROP TABLE IF EXISTS States")

    cur.execute("""CREATE TABLE IF NOT EXISTS States(
        ID SERIAL PRIMARY KEY,
        Name VARCHAR(40) UNIQUE
        )""")

    cur.execute("""CREATE TABLE IF NOT EXISTS Sites(
        ID SERIAL,
        Name VARCHAR(128) UNIQUE,
        Type VARCHAR(128),
        State_ID INTEGER REFERENCES States(ID),
        Location VARCHAR(255),
        Description TEXT
        )""")

    conn.commit()
    print("setup success")



# Write code / functions to deal with CSV files and insert data into the database here.

def insert(conn, cur, table, data_dict, no_return=False):
    """Accepts connection and cursor, table name, dictionary that represents one row, and inserts data into table. (Not the only way to do this!)"""
    column_names = data_dict.keys()
    #print(column_names, "column_names") # for debug
    if not no_return:
        query = sql.SQL('INSERT INTO {0}({1}) VALUES({2}) ON CONFLICT DO NOTHING RETURNING id').format(
            sql.SQL(table),
            sql.SQL(', ').join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(map(sql.Placeholder, column_names))
        )
    else:
        query = sql.SQL('INSERT INTO {0}({1}) VALUES({2}) ON CONFLICT DO NOTHING').format(
            sql.SQL(table),
            sql.SQL(', ').join(map(sql.Identifier, column_names)),
            sql.SQL(', ').join(map(sql.Placeholder, column_names))
        )
    query_string = query.as_string(conn) # thanks to sql module
    cur.execute(query_string, data_dict) # will mean that id is in cursor, because insert statement returns id in this function
    if not no_return:
        return cur.fetchone()['id']

def csv_to_db(statename):
    state_id = insert(conn, cur, "States", {"name" : statename})
    filename = statename + '.csv'
    with open(filename, newline = '', encoding = 'utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row_dict in reader:
            # print(row_dict)
            del row_dict['ADDRESS']
            lower_dict = dict((k.lower(), v) for k, v in row_dict.items() if k != None)
            lower_dict['state_id'] = state_id
            insert(conn, cur, "Sites", lower_dict, True)
    conn.commit()
    print("insert success")


# Make sure to commit your database changes with .commit() on the database connection.



# Write code to be invoked here (e.g. invoking any functions you wrote above)

conn, cur = get_connection_and_cursor()
set_up_db()
csv_to_db('arkansas')
csv_to_db('california')
csv_to_db('michigan')

# Write code to make queries and save data in variables here.


cur.execute('SELECT location FROM sites')
all_locations = cur.fetchall()
print(type(all_locations))

cur.execute(""" SELECT name FROM sites WHERE description LIKE '%beautiful%' """) # when passing a string val to postgres, single quote should be used
beautiful_sites = cur.fetchall()
print(beautiful_sites)

cur.execute(""" SELECT COUNT(*) FROM SITES WHERE TYPE = 'National Lakeshore' """)
natl_lakeshores = cur.fetchall()
print(natl_lakeshores)

cur.execute(""" SELECT SITES.NAME FROM SITES INNER JOIN STATES ON (SITES.STATE_ID = STATES.ID) WHERE STATES.NAME = 'michigan' """)
michigan_names = cur.fetchall()
print(michigan_names)

cur.execute(""" SELECT COUNT(*) FROM SITES INNER JOIN STATES ON (SITES.STATE_ID = STATES.ID) WHERE STATES.NAME = 'arkansas' """)
total_number_arkansas = cur.fetchall()
print(total_number_arkansas)


# We have not provided any tests, but you could write your own in this file or another file, if you want.
