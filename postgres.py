import psycopg2
from configparser import ConfigParser
from pyspark.sql import SparkSession

def configpostgres(filename="detailconfig.ini",
           section = "postgresql"):
    #create a parser
    parser = ConfigParser()
    #read config file
    parser.read(filename)
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section{0} is not found in the {1} file.'
                        .format(section, filename))
    return db

def connectpostgres():
    connection = None
    try:
        params = configpostgres()
        print(f"[INFO] SUCCESS connecting postgreSQL database ...")
        connection = psycopg2.connect(**params)

        #create a cursor
        crsr = connection.cursor()
        print('PostgreSQL database version: ')
        crsr.execute('SELECT version()')
        db_version = crsr.fetchone()
        print(db_version)
        crsr.close
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)
 
if __name__== "__main__":
    connectpostgres()


