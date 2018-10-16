import geoip2.database
import psycopg2
import sys


def get_GeoliteDB():
    return geoip2.database.Reader("GeoLite2-City.mmdb")

def get_DataBaseCon():
    conn_string = "host='localhost' dbname='udeg' user='postgres' password='escire2018'"
    conn = psycopg2.connect(conn_string)
    return conn.cursor()

def get_Title(cursor, id):
    query = "SELECT text_value FROM metadatavalue, (SELECT * FROM handle WHERE resource_id="+id+") AS hndl WHERE metadatavalue.resource_id=hndl.resource_id AND metadata_field_id=64 AND metadatavalue.resource_type_id=hndl.resource_type_id"
    cursor.execute(query)
    records = cursor.fetchall()
    if records:
        return records[0][0].replace(",", " - ")
    else:
        return id

def get_City(cursor, ip):
    try:
        respuesta = cursor.city(ip)
        if respuesta.subdivisions.most_specific.name:
            return respuesta.subdivisions.most_specific.name+","+respuesta.country.iso_code
        else:
            return respuesta.country.name
    except geoip2.errors.AddressNotFoundError:
        return "Unknown"

def format_date(date):
    n_date = date[:7]
    return n_date.replace("-","")

def read_files(Nombre):
    f = open(Nombre, "r")
    return [l.replace("\n","") for l in f.readlines()]

def write_file(Nombre):
    f = open("Out/"+Nombre, "w")
    return f

if __name__ == '__main__':
    gdb = get_GeoliteDB()
    dbc = get_DataBaseCon()
    f = write_file("statistics.log.2018")
    for l in read_files("Archivos/statistics.log.2018"):
        cadena_arr = l.split(",")
        cadena_arr[3] = format_date(cadena_arr[3])
        title = get_Title(dbc, cadena_arr[2])
        City = get_City(gdb, cadena_arr[5])
        cadena_arr[2] = title
        if City:
            cadena_arr[5] = City
        else:
            cadena_arr[5] = "Unknown, Unknown"
        f.write(",".join(cadena_arr)+"\n")

