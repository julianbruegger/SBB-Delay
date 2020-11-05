import requests
import json
import math
import time
import mysql.connector
import dateutil.parser


# Define db
mydb = mysql.connector.connect(
    host="192.168.111.61", 
    user="sbb",
    password="a5SDhX2qhauOz31n",
    database="sbb")
mycursor = mydb.cursor()

url = "https://data.sbb.ch/api/records/1.0/search/?dataset=ist-daten-sbb&q=&rows=1000&facet=betreiber_id&facet=produkt_id&facet=linien_id&facet=linien_text&facet=verkehrsmittel_text&facet=faellt_aus_tf&facet=bpuic&facet=ankunftszeit&facet=an_prognose&facet=an_prognose_status&facet=ab_prognose_status&facet=ankunftsverspatung&facet=abfahrtsverspatung&refine.bpuic=8505000"

response = requests.get(url)
data = response.text
parsed = json.loads(data)

nhits = (parsed.get("nhits"))
print (nhits)

for i in range (nhits):
    abfahrtszeit = abfahrtszeit = str(parsed.get("records")[i].get("fields").get("abfahrtszeit"))
    abfahrtsverspatung = str(parsed.get("records")[i].get("fields").get("abfahrtsverspatung"))
    ab_prognose = str(parsed.get("records")[i].get("fields").get("ab_prognose")) 
    ankunftszeit = str(parsed.get("records")[i].get("fields").get("ankunftszeit"))
    betriebstag = str(parsed.get("records")[i].get("fields").get("betriebstag"))
    verkehrsmittel_text = str(parsed.get("records")[i].get("fields").get("verkehrsmittel_text"))
    faellt_aus_tf = str(parsed.get("records")[i].get("fields").get("faellt_aus_tf"))
    linien_text= str(parsed.get("records")[i].get("fields").get("linien_text"))
    an_prognose = str(parsed.get("records")[i].get("fields").get("an_prognose"))

    #Inser values into db
    sql = "INSERT INTO delay_LU (abfahrtszeit, ab_prognose, abfahrtsverspatung, ankunftszeit, betriebstag,verkehrsmittel_text, faellt_aus_tf, linien_text, an_prognose) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    val = (abfahrtszeit, ab_prognose, abfahrtsverspatung, ankunftszeit, betriebstag, verkehrsmittel_text, faellt_aus_tf, linien_text, an_prognose)
    mycursor.execute(sql, val)
    mydb.commit()
