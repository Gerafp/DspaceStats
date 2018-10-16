import psycopg2
import sys


def main():
    conn_string = "host='localhost' dbname='udeg' user='postgres' password='escire2018'"
    conn = psycopg2.connect(conn_string)
    cursor = conn.cursor()
    cursor.execute("SELECT text_value FROM metadatavalue, (SELECT * FROM handle WHERE handle_id=63341) AS hndl WHERE metadatavalue.resource_id=hndl.resource_id AND metadata_field_id=64 AND metadatavalue.resource_type_id=hndl.resource_type_id")

    records = cursor.fetchall()

    print(records[0][0])

if __name__ == '__main__':
    main()