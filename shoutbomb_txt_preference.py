#!/usr/local/bin/python3
#
# Script to export patron notice preferences from Sierra and send them to Shoutbomb
#

import os
import psycopg2

from datetime import date, datetime, timedelta
from ftplib import  FTP

from mvsettings import *

def strify(obj):
    if obj is None:
        return ''
    else:
        return obj
        
def write_file(mycursor, filename_template, title_row, query):
    filename = (filename_template % (date.today().strftime('%m%d%y'),))
    try:
        mycursor.execute(query)
        rows = mycursor.fetchall()

        f = open(filename, "w")
    
        f.write(title_row)
        f.write("\n")

        for r in rows:
            f.write("|".join(map(strify, r)))
            f.write("\n")

        f.close()
    except Exception as my1e:
        print(str(emy1e))
        return None
    return filename


def put_file(myftps, filename, directory):
    try:
        if filename is not None:
            print ("sending " + filename)
            f = open(filename, 'rb')
            myftps.storbinary(("STOR /%s/%s" % (directory, filename,)), f)
            f.close()
    except Exception as my2e:
        print(str(my2e))

try:
    conn = psycopg2.connect("dbname='%s' user='%s' host='%s' port='1032' password='%s' sslmode='require'" % (DB_NAME, DB_USER, DB_HOST, DB_PASSWORD,))
except psycopg2.Error as e:
    print ("Unable to connect to database: " + str(e))

cursor = conn.cursor()

column_titles = "phone|barcode"
sms_q = """SELECT vv1.field_content AS phone_number, pv.barcode AS barcode
    FROM sierra_view.patron_view AS pv
    JOIN sierra_view.varfield_view AS vv1 ON (pv.record_num = vv1.record_num)
    JOIN sierra_view.varfield_view AS vv2 ON (pv.record_num = vv2.record_num)
    WHERE vv1.record_type_code = 'p'
    AND vv1.varfield_type_code = 'o' AND vv2.varfield_type_code = 'e'
    AND (vv2.field_content LIKE 't' OR vv2.field_content LIKE 'T')
    ORDER BY barcode;"""

voice_q = """SELECT prp.phone_number AS phone_number, pv.barcode AS barcode
    FROM sierra_view.patron_view AS pv
    JOIN sierra_view.patron_record_phone AS prp ON (pv.id = prp.patron_record_id)
    JOIN sierra_view.varfield_view AS vv ON (pv.record_num = vv.record_num)
    WHERE prp.patron_record_phone_type_id = 1
    AND vv.record_type_code = 'p' AND vv.varfield_type_code = 'e'
    AND (vv.field_content LIKE 'p' OR vv.field_content LIKE 'P') AND pv.barcode is not null
    ORDER BY pv.barcode;"""

os.chdir(SHOUTBOMB_DIR)
os.system("move sms*.txt archive/")
os.system("move voice*.txt archive/")

archive_limit = datetime.today() - timedelta(days=30)

for f in os.listdir("archive/"):
    fullpath = os.path.abspath("archive/" + f)
    ctime = datetime.fromtimestamp(os.stat(fullpath).st_ctime)
    if ctime < archive_limit and f.endswith(".txt"):
        print "deleting: " + fullpath
        os.remove(fullpath)

sms_preference_file = write_file(cursor, "sms%s.txt", column_titles, sms_q)
print ("created %s" % sms_preference_file)
voice_preference_file = write_file(cursor, "voice%s.txt", column_titles, voice_q)
print ("created %s" % voice_preference_file)

try:
   ftps = FTP(SHOUTBOMB_HOST, SHOUTBOMB_USER, SHOUTBOMB_PASSWORD)
   ftps.login(SHOUTBOMB_USER, SHOUTBOMB_PASSWORD)
   put_file(ftps, sms_preference_file, "/text_patrons/")
   put_file(ftps, voice_preference_file, "/voice_patrons/")

   ftps.quit()
except Exception as mye:
    print(str(mye))
