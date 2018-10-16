import pygeoip
import geoip2.database

#gi4 = pygeoip.GeoIP("GeoLiteCity.dat", pygeoip.MEMORY_CACHE)

#print(gi4.record_by_addr("35.202.99.105")['country_name'])


reader = geoip2.database.Reader("GeoLite2-City.mmdb")
respuesta = reader.city("201.162.191.38")
if respuesta.subdivisions.most_specific.name:
    print(respuesta.subdivisions.most_specific.name+"\t"+respuesta.country.name)
else:
    print(respuesta.country.name)
