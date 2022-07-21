#!/usr/local/bin/python3
#
# Script to export overdues and courtesy notices from Sierra and send them to shoutbomb
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
        return str(obj)
        
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
            print (str(e))

        f.close()
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

overdues_titles = "patron_no|item_barcode|title|due_date|item_no|money_owed|loan_rule|item_holds|bib_holds|renewals|bib_no"
overdues_q =  """SELECT 'p' || rmp.record_num || 'a' AS patron_no, 
                 replace(ib.field_content,' ','') AS item_barcode, 
                 TRIM(TRAILING '/' from COALESCE(s.content, v.field_content)) AS title, 
                 to_char(c.due_gmt,'MM-DD-YYYY') AS due_date, 
                 'i' || rmi.record_num || 'a' AS item_no, 
                 round(p.owed_amt,2) AS money_owed, 
                 c.loanrule_code_num AS loan_rule, 
                 nullif (count(ih.id),0) AS item_holds, 
                 nullif (count(bh.id),0) AS bib_holds, 
                 c.renewal_count AS renewals, 'b' || rmb.record_num || 'a' AS bib_no 
  FROM sierra_view.checkout AS c 
  JOIN sierra_view.patron_record AS p ON (p.id = c.patron_record_id) 
  JOIN sierra_view.record_metadata AS rmp ON (rmp.id = c.patron_record_id AND rmp.record_type_code = 'p') 
  JOIN sierra_view.item_record AS i ON (i.id = c.item_record_id) 
  JOIN sierra_view.record_metadata AS rmi ON (rmi.id = i.id AND rmi.record_type_code = 'i') 
  JOIN sierra_view.varfield AS ib ON (ib.record_id = i.id AND ib.varfield_type_code = 'b') 
  JOIN sierra_view.bib_record_item_record_link AS bil ON (bil.item_record_id = i.id) 
  LEFT JOIN sierra_view.subfield AS s ON (s.record_id = bil.bib_record_id AND s.marc_tag='245' AND s.tag = 'a') 
  LEFT JOIN sierra_view.varfield AS v ON (v.record_id = bil.bib_record_id AND v.varfield_type_code = 't' AND v.marc_tag IS NULL)
  LEFT JOIN sierra_view.hold as bh ON (bh.record_id = bil.bib_record_id) 
  LEFT JOIN sierra_view.hold as ih ON (ih.record_id = i.id and ih.status = '0') 
  LEFT JOIN sierra_view.record_metadata as rmb ON (rmb.id = bil.bib_record_id AND rmb.record_type_code = 'b') 
  WHERE (current_date - c.due_gmt::date) >= 1 AND (current_date - c.due_gmt::date) < 31
  GROUP BY 1,2,3,4,5,6,7,10,11 
  ORDER BY patron_no"""

renewals_titles = "patron_no|item_barcode|title|due_date|item_no|money_owed|loan_rule|item_holds|bib_holds|renewals|bib_no"
renewals_q = """SELECT 'p' || rmp.record_num || 'a' AS patron_no,
                replace(ib.field_content,' ','') AS item_barcode,
                TRIM(TRAILING '/' from COALESCE(s.content, v.field_content)) AS title,
                to_char(c.due_gmt,'MM-DD-YYYY') AS due_date,
                'i' || rmi.record_num || 'a' AS item_no,
                round(p.owed_amt,2) AS money_owed,
                c.loanrule_code_num AS loan_rule,
                nullif (count(ih.id),0) AS item_holds,
                nullif (count(bh.id),0) AS bib_holds,
                c.renewal_count AS renewals,
                'b' || rmb.record_num || 'a' AS bib_no
  FROM sierra_view.checkout AS c
  JOIN sierra_view.patron_record AS p ON (p.id = c.patron_record_id)
  JOIN sierra_view.record_metadata AS rmp ON (rmp.id = c.patron_record_id AND rmp.record_type_code = 'p')
  JOIN sierra_view.item_record AS i ON (i.id = c.item_record_id)
  JOIN sierra_view.record_metadata AS rmi ON (rmi.id = i.id AND rmi.record_type_code = 'i')
  JOIN sierra_view.varfield AS ib ON (ib.record_id = i.id AND ib.varfield_type_code = 'b')
  JOIN sierra_view.bib_record_item_record_link AS bil ON (bil.item_record_id = i.id)
  LEFT JOIN sierra_view.subfield AS s ON (s.record_id = bil.bib_record_id AND s.marc_tag='245' AND s.tag = 'a')
  LEFT JOIN sierra_view.varfield AS v ON (v.record_id = bil.bib_record_id AND v.varfield_type_code = 't' AND v.marc_tag IS NULL)
  LEFT JOIN sierra_view.hold as bh ON (bh.record_id = bil.bib_record_id) 
  LEFT JOIN sierra_view.hold as ih ON (ih.record_id = i.id and ih.status = '0')         
  LEFT JOIN sierra_view.record_metadata as rmb ON (rmb.id = bil.bib_record_id AND rmb.record_type_code = 'b')
  WHERE (c.due_gmt::date - current_date) in (0,1,2)
  GROUP BY 1,2,3,4,5,6,7,10,11
  ORDER BY patron_no"""

os.chdir(SHOUTBOMB_DIR)
os.system("move overdue*.txt archive/")
os.system("move renew*.txt archive/")

archive_limit = datetime.today() - timedelta(days=30)

for f in os.listdir("archive/"):
    fullpath = os.path.abspath("archive/" + f)
    ctime = datetime.fromtimestamp(os.stat(fullpath).st_ctime)
    if ctime < archive_limit and f.endswith(".txt"):
        print ("deleting: " + fullpath)
        os.remove(fullpath)

overdue_file = write_file(cursor, "overdue%s.txt", overdues_titles, overdues_q)
print ("created %s" % overdue_file)
renewals_file = write_file(cursor, "renew%s.txt", renewals_titles, renewals_q)
print ("created %s" % renewals_file)

try:
   ftps = FTP(SHOUTBOMB_HOST, SHOUTBOMB_USER, SHOUTBOMB_PASSWORD)
   ftps.login(SHOUTBOMB_USER, SHOUTBOMB_PASSWORD)
   put_file(ftps, overdue_file, "/Overdue/")
   put_file(ftps, renewals_file, "/Renew/")

   ftps.quit()
except Exception as e:
   print (str(e))
