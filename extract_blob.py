
#!/usr/bin/env python3
import psycopg2
import yaml
import sys, os
import credentials as cfg

connection = None

def write_file(data, filename):
    with open(filename, 'wb') as file:
        file.write(data)
        #file.write(bytes(data, 'UTF-8'))


def getConnection():
    global connection

    if not connection:
        try:
            connection = psycopg2.connect(user = cfg.postgres['user'],
                                        password = cfg.postgres['passwd'],
                                        host = cfg.postgres['host'],
                                        port = cfg.postgres['port'],
                                        database = cfg.postgres['db'])

        except (Exception, psycopg2.Error) as error :
            print ("Error while connecting to PostgreSQL", error)


    return connection


def closeConnection():
    global connection
    if(connection):
        connection.close()
        #print("PostgreSQL connection is closed")


def extract_blob(table, blob_field):
    connection = getConnection()
    cursor = connection.cursor()
    
    sql_fetch_blob_query = "SELECT * from " + table
    cursor.execute(sql_fetch_blob_query)
    records = cursor.fetchall()
    for row in records:
        filename = os.path.join(cfg.storage_folter, str(row[0]) + ".jpg");  
        photo_data = row[1]
        print("Storing image", row[0], "on", filename)
        write_file(photo_data, filename)

    cursor.close()

def main():
    extract_blob('photos', 'photo')
    closeConnection()


if __name__ == '__main__':
  main()
  
