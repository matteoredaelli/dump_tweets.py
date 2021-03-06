#!/usr/bin/env python

## This program is free software: you can redistribute it and/or modify
## it under the terms of the GNU General Public License as published by
## the Free Software Foundation, either version 3 of the License, or
## (at your option) any later version.

## This program is distributed in the hope that it will be useful,
## but WITHOUT ANY WARRANTY; without even the implied warranty of
## MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
## GNU General Public License for more details.

## You should have received a copy of the GNU General Public License
## along with this program.  If not, see <http://www.gnu.org/licenses/>.

## AUTHOR:           matteo DOT redaelli AT libero DOT it
## AUTHOR'S WEBSITE: http://www.redaelli.org/matteo/

## This file is part of the project dump_tweets.py, hosted at https://github.com/matteoredaelli/dump_tweets.py


__VERSION__="1.0-SNAPSHOT"
__AUTHORS__ = "Matteo Redaelli,  matteo DOT redaelli AT libero DOT it"
__LICENSE__ = "GPLV3+"

#import argparse   # valid for python >= 2.7
from datetime import datetime
import calendar
import getopt
import HTMLParser
import json
import os.path
import rfc822
import string
import sys
import time
import urllib
import urllib2
import MySQLdb
import re
 
def create_table(db_cursor, tablename):
    sql_statement = """CREATE TABLE %s (
  id varchar(30) NOT NULL,
  PRIMARY KEY(id),
  from_user varchar(30) NOT NULL,
  timestamp int(11) NOT NULL,
  text varchar(250) NOT NULL,
  iso_language_code char(2) default NULL,
  geo_lat double(10,6) default NULL,
  geo_long double(10,6) default NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8""" % (tablename)
    db_cursor.execute(sql_statement.encode('utf8'))

def drop_table(db_cursor, tablename):
    sql_statement = """DROP TABLE %s""" % (tablename)
    db_cursor.execute(sql_statement.encode('utf8'))

def my_exit(conn=False, db_cursor = False):
    if db_cursor != False:
        db_cursor.close()
    if conn != False:
        conn.close()
    sys.exit(1)

def usage():
    print
    print "Software   : %s" %  __file__
    print "Version    : %s" %  __VERSION__
    print "License    : %s" %  __LICENSE__
    print "Author(s)  : %s" %  __AUTHORS__
    print
    print "USAGE: %s -q|--query string [-f|--file filename] [-i|--since_id number] [-v|--verbose] [-s|--slip number] [-r|--result_type string] [-d|--db dbhost:dbport:dbuser:dbpass:dbtable] [-C|--create-table] [-D|--drop-table]" % __file__
    print
    print "       -q string: string is the search string"
    print "       -d string: the tweets are saved to a MySQL db. strig is like 'dbhost:dbport:dbuser:dbpass:dbtable'"
    print "       -f string: max_id is saved to a file, it will be used for incremental dumping after a restart"
    print "       -r string: result_type"
    print "       -s number: the script runs forever, it retrives tweets each number seconds"
    print

def clean_string(s):
    s = HTMLParser.HTMLParser().unescape(s)
    s = urllib.unquote(s)
    s = s.strip()
    return s

def dump_tweets(q, since_id=0, verbose=True, rpp=100, result_type = 'mixed', db_cursor=False, db_table=False):
    base_url = "http://search.twitter.com/search.json"
    query = "?" + urllib.urlencode({'q' : q,
                                    'since_id'        : since_id,
                                    'rpp'             : rpp,
                                    'result_type'     : result_type,
                                    'page'            : 1,
                                    'include_entities': 1
                                    })

    max_id = counter = 0
    
    for c in range(1, 15):
        url = base_url + query
        if verbose:
            print >> sys.stderr, url
        raw_response = urllib2.urlopen(url)
        json_response = json.load(raw_response)
        max_id = json_response["max_id"]
        raw_response.close()

        all_tweets = json_response["results"]
        counter = counter + len(all_tweets)
        
        for tweet in all_tweets:
            print tweet
            id = tweet["id"]
            timestamp = calendar.timegm(rfc822.parsedate(tweet["created_at"]))
            from_user = clean_string(tweet["from_user"])
            ##from_user_id = clean_string(tweet["from_user_id"])
            text = clean_string(tweet["text"])
            ##iso_language_code = ""
            iso_language_code = tweet["iso_language_code"]

            ## "geo":{"coordinates":[48.748530,2.448800],"type":"Point"}
            if tweet["geo"]:
                geo_lat = tweet["geo"]["coordinates"][0]
                geo_long =  tweet["geo"]["coordinates"][1]
            else:
                geo_lat = 0.0
                geo_long = 0.0
            row = str(id) + " : " + str(timestamp) + " : " + from_user + " : " + text + " : " + iso_language_code
            print row.encode('utf8')
            if db_cursor != False:
                sql_statement = u"""insert into %s (id, from_user, timestamp, text, iso_language_code, geo_lat, geo_long) values (%d, '%s', %d, '%s', '%s', %f, %f)""" %  (db_table, id, from_user, timestamp, text.replace("'","\\'"), iso_language_code, geo_lat, geo_long)
                ##print >> sys.stderr, sql_statement
                try:
                    db_cursor.execute(sql_statement.encode('utf8'))
                    db_cursor.connection.commit()
                except MySQLdb.Error, e:
                    print >> sys.stderr, "Error %d: %s" % (e.args[0], e.args[1])
                    print >> sys.stderr, "Skipping inserting this tweet to the DB"
                    
        ##print json_response["next_page"]

        if "next_page" in json_response.keys():
            query = json_response["next_page"]
        else:
            break

    result = dict()
    result["max_id"] = max_id
    result["counter"] = counter
    return result


def rep_dump_tweets(q, since_id=0, sleep=3600, verbose=True, rpp=100, result_type = 'mixed', filename=False, db_cursor=False, db_table=False):
    while True:
        if verbose:
            print >> sys.stderr, "since_id = " + str(since_id)
        result = dump_tweets(q=q, since_id=since_id, verbose=verbose, db_cursor=db_cursor, db_table=db_table)
        max_id = result["max_id"]
        counter = result["counter"]
        if verbose:
            print >> sys.stderr, "retreived tweets = " + str(counter)
            print >> sys.stderr, "max_id = " + str(max_id)
        since_id = max_id
        if filename != False:
            id_to_file(since_id, filename)
        if sleep == 0:
            break
        time.sleep(sleep)

def id_from_file(filename):
    f = open(filename, 'r')
    row = f.readline().strip()
    f.close()
    if row == "":
        id = 0
    else:
        id = int(row)
        
    return id

def id_to_file(id, filename):
    f = open(filename, 'w')
    f.write(str(id))
    f.close()
    
def main():
    try:
        opts, args = getopt.getopt(sys.argv[1:], "CDd:f:hi:q:r:s:v", 
                                   ["create-table", "drop-table", "db=", "file=", "help", "query=", "result_type=", "since_id=", "sleep=", "verbose"])
    except getopt.GetoptError, err:
        usage()
        sys.exit(2)

    q = ""
    since_id = 0
    result_type = "mixed"
    sleep = 0
    filename = tablename = action = False
    verbose = False

    db = conn = db_cursor = db_table = False

    
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-d", "--database"):
            db = string.split(a,":")
        elif o in ("-f", "--file"):
            filename = a
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-i", "--since_id"):
            since_id = int(a)
        elif o in ("-q", "--query"):
            q = a
        elif o in ("-r", "--result_type"):
            result_type = a
        elif o in ("-s", "--sleep"):
            sleep = int(a)
        elif o in ("-C", "--create_table"):
            action = "create"
        elif o in ("-D", "--drop_table"):
            action = "drop"
        else:
            assert False, "unhandled option"

    db_cursor = False
    db_table = False
    
    if not (db == False):
        if len(db) != 6:
            print("-d option is wrong.")
            usage()
            
        db_host = db[0]
        db_port = db[1]
        db_name = db[2]
        db_user = db[3]
        db_pass = db[4]
        db_table = db[5]

 
        try:
            conn = MySQLdb.connect (host = db_host,
                                    user = db_user,
                                    passwd = db_pass,
                                    db = db_name,
                                    charset = "utf8",
                                    use_unicode=True)
            db_cursor = conn.cursor()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            usage()
            my_exit()
    
    if action == "create":
        create_table(db_cursor, db_table)
        my_exit(conn=conn, db_cursor = db_cursor)


    if action == "drop":
        drop_table(db_cursor, db_table)
        my_exit(conn=conn, db_cursor = db_cursor)

    if q == "":
        print("Required -q option is missing. No search text no party!")
        usage()
        my_exit(conn=conn, db_cursor = db_cursor)
        
    if filename != False:
        if os.path.isfile(filename):
            if verbose:
                print >> sys.stderr, "Reading since_id from file " + filename
            since_id = id_from_file(filename) 
    
    rep_dump_tweets(q=q,
                    since_id=since_id,
                    sleep=sleep,
                    verbose=verbose,
                    result_type=result_type,
                    filename=filename,
                    db_cursor = db_cursor,
                    db_table=db_table)
    my_exit(conn=conn, db_cursor = db_cursor)
    
if __name__ == "__main__":
    main()

