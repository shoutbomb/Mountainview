#!/usr/local/bin/python3
#
# Script to export holds, overdues and courtesy notices from Sierra and send them to shoutbomb
# Python script by Esther Verreau (aside from slight tweaks)
# SQL Queries based on those from Gerri Moeller at OWLS
# Last modified by Gem Stone-Logan (gem.stone-logan@mountainview.gov)
#

import os
import psycopg2

from datetime import date, datetime, timedelta
from ftplib import  FTP

from mvsettings import *

def strify(obj):
    if obj == None:
        return ''
    else:
        return obj
        
def write_file(cursor, filename_template, title_row, query):
    filename = (filename_template % (date.today().strftime('%m%d%y'),))
    try:
        cursor.execute(query)
        rows = cursor.fetchall()

        f = open(filename, "w", encoding='utf-8')
    
        f.write(title_row)
        f.write("\n")

        try:
            for r in rows:
                f.write("|".join(map(strify, r)))
                f.write("\n")
        except Exception as e:
            print ("for loop didn't work " + str(e))

        try:
            f.close()
        except:
            print ("did not close")
    except:
        return None
    return filename


def put_file(ftps, filename, directory):
    try:
        if filename != None:
            print ("sending " + filename)
            f = open(filename, 'rb')
            ftps.storbinary(("STOR /%s/%s" % (directory, filename,)), f)
            f.close()
    except Exception as e:
        print (str(e))

try:
    conn = psycopg2.connect("dbname='%s' user='%s' host='%s' port='1032' password='%s' sslmode='require'" % (DB_NAME, DB_USER, DB_HOST, DB_PASSWORD,))
except psycopg2.Error as e:
    print ("Unable to connect to database: " + str(e))

cursor = conn.cursor()

holds_titles = "title|last_update|item_no|patron_no|pickup_location"
holds_q = """SELECT TRIM(TRAILING '/' from COALESCE(s.content, v.field_content)), 
             to_char(rmi.record_last_updated_gmt,'MM-DD-YYYY') AS last_update, 
             'i' || rmi.record_num || 'a' AS item_no, 
             'p' || rmp.record_num || 'a' AS patron_no, 
             h.pickup_location_code AS pickup_location,
             irp.barcode AS item_barcode
   FROM sierra_view.hold AS h JOIN sierra_view.patron_record AS p ON ( p.id = h.patron_record_id ) 
   JOIN sierra_view.record_metadata AS rmp ON (rmp.id = h.patron_record_id AND rmp.record_type_code = 'p') 
   JOIN sierra_view.item_record AS i ON ( i.id = h.record_id ) 
   JOIN sierra_view.bib_record_item_record_link AS bil ON ( bil.item_record_id = i.id AND bil.bibs_display_order = 0 ) 
   LEFT JOIN sierra_view.item_record_property AS irp ON irp.item_record_id=i.id
   LEFT JOIN sierra_view.subfield AS s ON (s.record_id = bil.bib_record_id AND s.marc_tag='245' AND s.tag = 'a') 
   LEFT JOIN sierra_view.varfield AS v ON (v.record_id = bil.bib_record_id AND v.varfield_type_code = 't' AND v.marc_tag IS NULL)
   JOIN sierra_view.record_metadata AS rmi ON ( rmi.id = i.id AND rmi.record_type_code = 'i') 
   WHERE i.item_status_code in ( '!', '#')
   AND h.status in ('b','i','0')
   AND h.pickup_location_code IS NOT NULL
   AND NOW()-rmi.record_last_updated_gmt > interval '1' hour"""

os.chdir(SHOUTBOMB_DIR)
os.system("move holds*.txt archive/")

archive_limit = datetime.today() - timedelta(days=30)

for f in os.listdir("archive/"):
    fullpath = os.path.abspath("archive/" + f)
    ctime = datetime.fromtimestamp(os.stat(fullpath).st_ctime)
    if ctime < archive_limit and f.endswith(".txt"):
        print ("deleting: " + fullpath)
        os.remove(fullpath)

holds_file = write_file(cursor, "holds%s.txt", holds_titles, holds_q)
print ("created %s" % holds_file)

try:
   ftps = FTP(SHOUTBOMB_HOST, SHOUTBOMB_USER, SHOUTBOMB_PASSWORD)
   ftps.login(SHOUTBOMB_USER, SHOUTBOMB_PASSWORD)
   put_file(ftps, holds_file, "/Holds/")

   ftps.quit()
except Exception as e:
   print (str(e))
