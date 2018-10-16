#
# Script para procesar las estadisticas de los repositorios DSpace
#

import geoip2.database
import psycopg2
import sys
import os
from progress.bar import Bar

# Database connection

HOSTNAME = 'localhost'
DBNAME = 'udeg'
USER = 'postgres'
PASS = 'escire2018'

def get_GeoliteDB():
    return geoip2.database.Reader("Geolite/GeoLite2-City.mmdb")

def get_DataBaseCon():
    conn_string = "host='"+ HOSTNAME +"' dbname='"+ DBNAME +"' user='"+ USER +"' password='"+ PASS + "'"
    conn = psycopg2.connect(conn_string)
    if conn:
        return conn.cursor()
    else:
        print("Fallo en la conexion de la BD")
        exit()

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
        query_answer = cursor.city(ip)
        if query_answer.subdivisions.most_specific.name:
            return query_answer.subdivisions.most_specific.name+","+query_answer.country.iso_code
        elif query_answer.country.name and query_answer.country.iso_code:
            return query_answer.country.name + "," + query_answer.country.iso_code
        else:
            return query_answer.country.name

    except geoip2.errors.AddressNotFoundError:
        return "Unknown"

def format_date(date):
    n_date = date[:7]
    return n_date.replace("-","")


def in_directory_exists(in_path):
    if os.path.exists(in_path):
        return [f for f in os.listdir(in_path)]
    else:
        print("No existe " + in_path)
        exit()

def get_out_path_file(out_path, file_name):
    if not os.path.exists(out_path):
        os.mkdir(out_path)
    return os.path.join(out_path, file_name)

def get_in_path_file(in_path, file_name):
    return os.path.join(in_path,file_name)

def logs_procesing(in_file_path, gdb, dbc, out_file_path):
    f_In = open(in_file_path, "r")
    f_Out = open(out_file_path, "w")
    lineas = [l.replace("\n","") for l in f_In.readlines()]
    bar = Bar('Processing', max=len(lineas))
    for l in lineas:
        cadena_arr = l.split(",")
        cadena_arr[3] = format_date(cadena_arr[3])

        title = get_Title(dbc, cadena_arr[2])
        City = get_City(gdb, cadena_arr[5])

        cadena_arr[2] = title
        if City:
            cadena_arr[5] = City
        else:
            cadena_arr[5] = "UNK,UNK"
        f_Out.write(",".join(cadena_arr)+"\n")
        bar.next()
    f_In.close()
    f_Out.close()
    bar.finish()

def log_separing(out_path, file_name):
    f_itm = file_name + "-item.txt"
    f_bts = file_name + "-bitstream.txt"
    f_com = file_name + "-communities.txt"
    f_col = file_name + "-collection.txt"

    f_out1 = open(os.path.join(out_path, f_itm), "w")
    f_out2 = open(os.path.join(out_path, f_bts), "w")
    f_out3 = open(os.path.join(out_path, f_com), "w")
    f_out4 = open(os.path.join(out_path, f_col), "w")
    f_in = open(os.path.join(out_path, file_name), "r")
    lineas = f_in.readlines()

    bar = Bar('Processing', max=len(lineas))
    for l in lineas:
        aux = l.split(",")
        if aux[1] == "view_bitstream":
            f_out2.write(l)
        elif aux[1] == "view_community":
            f_out3.write(l)
        elif aux[1] == "view_collection":
            f_out4.write(l)
        else:
            f_out1.write(l)
        bar.next()

    f_out1.close()
    f_out2.close()
    f_out3.close()
    f_out4.close()
    f_in.close()
    os.remove(os.path.join(out_path, file_name))
    bar.finish()
    

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("El modo de uso es python Stats_procesing.py path_inicio path_salida")
        exit()
    else:
        in_path = sys.argv[1]
        out_path = sys.argv[2]

    gdb = get_GeoliteDB()
    dbc = get_DataBaseCon()
    in_path_files = in_directory_exists(in_path)

    for f in in_path_files:
        print("Procesando Archivo " + f)
        logs_procesing( get_in_path_file(in_path, f), gdb, dbc, get_out_path_file(out_path, f))
        print("Separando Archivo " + f)
        log_separing(out_path, f)