#-------------------------------------------------------------------------------
# Name:        ImportContatori
# Purpose:     trasferisce i dati contatori dal database sql server al database
#              contatori postgresql
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
    db = dbapi2.connect (host='<Indirizzo Ip Server>',database="<Nome database>", user="<username>", password="<password>")
except Exception, e:
    print e

cur = db.cursor()

server = "<Indirizzo Ip Server>"
user = "<username>"
pwd = "<password>"
db = "<Nome database>"

def main():
    conn = pymssql.connect(server, user, pwd, db)
    cursor = conn.cursor(as_dict=True)
    cursor.execute("SELECT * FROM dbo.vista_contatori_qgis")
    for row in cursor:
        ##print("Name=%s" % (row['nominativo']))
        Nominativo = row['nominativo']
        Indirizzo = row['indirizzofornitura']
        Civico = row['civicofornitura']
        Comune = row['comune']
        X = row['x']
        Y = row['y']
        Z = row['z']
        Utente = row['utente']
        Pratica = row['pratica']
        Matricola = row['matricolacontatore']
        Prodie = row['prodie']
        Tipologia = row['tipologieconsumo']
        Ubicazione = row['ubicazione']
        NrUtenze = row['numeroutenzefornitura']
        Depuratore = row['depuratore']
        Serbatoio = row['serbatoio']
        FognaNav = row['pagafognanav']
        FognaLet = row['pagafognaletturista']
        DepurazioneNav = row['pagadepurazionenav']
        DepurazioneLet = row['pagadepurazioneletturista']
        Letturista = row['letturista']
        Validato = row['validato']

        X = X.replace(",",".")
        Y = Y.replace(",",".")
        Z = Z.replace(",",".")
        x = X.split(".")[0]
        y = Y.split(".")[0]
        z = Z.split(".")[0]

        try:
            cur.execute("""SELECT * FROM qgis.contatori WHERE pratica=%s;""", (Pratica, ))
        except Exception, e:
            print e

        num = cur.rowcount

        if  num == 0:
            try:
                cur.execute("""INSERT INTO qgis.contatori(geom, nominativo, indirizzofornitura, civicofornitura, comune,
                                    x, y, z, utente, pratica, matricolacontatore, prodie, tipologieconsumo, ubicazione, numeroutenzefornitura,
                                    depuratore, serbatoio, pagafognanav, pagafognaletturista, pagadepurazionenav, pagadepurazioneletturista,
                                    letturista, validato)
                                VALUES (ST_GeomFromText('POINT(%s %s)', 32633), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);""",
                            (int(x), int(y), Nominativo, Indirizzo, Civico, Comune, X, Y, z, Utente, Pratica, Matricola, Prodie, Tipologia, Ubicazione,
                                 NrUtenze, Depuratore, Serbatoio, FognaNav, FognaLet, DepurazioneNav, DepurazioneLet, Letturista, Validato, ))
                db.commit()
                print ("insert ok.")
            except Exception, e:
                print e
                db.rollback()

    conn.close()

if __name__ == '__main__':
    main()

