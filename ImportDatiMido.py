#-------------------------------------------------------------------------------
# Name:        ImportDatiMido
# Purpose:     trasferisce i dati grandi utenze dal sito di Mido al database
#              grandi utenze postgresql
# Author:      michele.luca
#
# Created:     04/03/2015
# Copyright:   (c) michele.luca 2015
# Licence:     free
#-------------------------------------------------------------------------------
#!/usr/bin/env python
import requests
import xml.etree.ElementTree as ET
import psycopg2 as dbapi2
import string
import types

try:
    db = dbapi2.connect (host='<Indirizzo Ip Server>',database="<Nome database>", user="<username>", password="<password>")
except Exception, e:
    print e


url = '<Indirizzo URL pagina che espone i dati>'
data = {'User': '<username>', 'Password': '<password>', 'Tipo': '1', 'gg':'0'}
headers = {'content-type': 'application/x-www-form-urlencoded'}
proxies = {
  "http": "http://indirizzo proxy:porta",
}

def main():
    r = requests.post(url, params=data, headers=headers, proxies=proxies, stream=True)

    xmlfile = r.text

    tree = ET.fromstring(xmlfile)

    cur = db.cursor()

    for contatore in tree.findall('Contatore'):
        Pratica = contatore.find('Pratica').text
        Utente = contatore.find('Utente').text
        Matricola = contatore.find('Matricola').text
        Nome = contatore.find('Nome').text
        Localita = contatore.find('Localita').text
        Comune = contatore.find('Comune').text
        Provincia = contatore.find('Provincia').text
        Data = contatore.find('Data').text
        Lettura = contatore.find('Lettura').text
        Est = contatore.find('Est').text
        Nord = contatore.find('Nord').text

        #print(Pratica, Utente, Data, Lettura)

        Est = Est.replace(",",".")
        Nord = Nord.replace(",",".")
        x = Est.split(".")[0]
        y = Nord.split(".")[0]

        try:
            cur.execute("""SELECT * FROM qgis.grandi_utenze WHERE pratica=%s;""", (Pratica, ))
        except Exception, e:
            print e

        num = cur.rowcount

        if  num == 0:
            try:
                cur.execute("""INSERT INTO qgis.grandi_utenze(geom, pratica, utente, matricola, nome, localita, comune, provincia, data, lettura, est, nord)
                                VALUES (ST_GeomFromText('MULTIPOINT(%s %s)', 32633), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                            (int(x), int(y), Pratica, Utente, Matricola, Nome, Localita, Comune, Provincia, Data, Lettura, Est, Nord, ))
                db.commit()
                print ("insert ok.")
            except Exception, e:
                print e
                db.rollback()
        else:
            try:
                cur.execute("""UPDATE qgis.grandi_utenze
                               SET data=%s, lettura=%s
                             WHERE pratica=%s;""", (Data, Lettura, Pratica, ))
                db.commit()
                print ("update ok.")
            except Exception, e:
                print e
                db.rollback()

if __name__ == '__main__':
    main()


