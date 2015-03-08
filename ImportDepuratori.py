#-------------------------------------------------------------------------------
# Name:        ImportDepuratori
# Purpose:     trasferisce i dati dal database depuratori sql server al database
#              depuratori postgresql
# Author:      michele.luca
#
# Created:     04/03/2015
# Copyright:   (c) michele.luca 2015
# Licence:     free
#-------------------------------------------------------------------------------
#!/usr/bin/env python
import psycopg2 as dbapi2
import string
import types
import pymssql

try:
    db = dbapi2.connect (host='192.168.8.108',database="alsit", user="micheleluca", password="micheleluca")
except Exception, e:
    print e

cur = db.cursor()

server = "192.168.1.9"
user = "aqlucano"
pwd = "aqlucano2003"
db = "Depuratori"

def main():
    conn = pymssql.connect(server, user, pwd, db)
    cursor = conn.cursor(as_dict=True)
    cursor.execute('SELECT * FROM vista_depuratori_qgis')
    for row in cursor:
        ##print "ID=%s" % (row['idal'])
        idal = row['idal']
        comune = row['comune']
        localita = row['localita']
        corporecettore = row['corporecettore']
        bacino = row['bacinoidrografico']
        tipoimpianto = row['tipoimpianto']
        tipofognatura = row['tipofognatura']
        tiposcarico = row['tiposcarico']
        stato = row['stato']
        X = str(row['x'])
        Y = str(row['y'])
        Z = str(row['z'])
        abitanti = row['abitantiequivalenti']
        abitantifluttuanti = row['abitantiequivalentifluttuanti']
        abitantiprogetto = row['abitantiequivalentiprogetto']

        X = X.replace(",",".")
        Y = Y.replace(",",".")
        Z = Z.replace(",",".")
        x = X.split(".")[0]
        y = Y.split(".")[0]
        z = Z.split(".")[0]

        try:
            cur.execute("""SELECT * FROM qgis.depuratori WHERE idal=%s;""", (idal, ))
        except Exception, e:
            print e

        num = cur.rowcount

        if  num == 0:
            try:
                cur.execute("""INSERT INTO qgis.depuratori(idal, geom, comune, localita, corporecettore, bacinoidrografico,
                                   tipoimpianto, tipofognatura, tiposcarico, stato, x, y, z, abitantiequivalenti, abitantiequivalentifluttuanti, abitantiequivalentiprogetto)
                                VALUES (%s, ST_GeomFromText('POINT(%s %s)', 32633), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                            (idal, int(x), int(y), comune, localita, corporecettore, bacino, tipoimpianto, tipofognatura, tiposcarico,
                                 stato, x, y, z, abitanti, abitantifluttuanti, abitantiprogetto, ))
                db.commit()
                print ("insert ok.")
            except Exception, e:
                print e
                db.rollback()

    conn.close()

if __name__ == '__main__':
    main()
