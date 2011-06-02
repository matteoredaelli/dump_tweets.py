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

## AUTHOR:           matteo DOT redaelli AT libero DOT IT
## AUTHOR'S WEBSITE: http://www.redaelli.org/matteo/

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
 
def usage():
    print("Unknown option")
    
def clean_string(s):
    s = HTMLParser.HTMLParser().unescape(s)
    s = urllib.unquote(s)
    s = s.strip()
    return s

def dump_tweets(q, since_id=0, verbose=True, rpp=100, result_type = 'recent', db_cursor=False, db_table=False):
    base_url = "http://search.twitter.com/search.json"
    query = "?" + urllib.urlencode({'q' : q,
                                    'since_id'    : since_id,
                                    'rpp'         : rpp,
                                    'result_type' : result_type
                                    })
    
    while query != "":
        url = base_url + query
        print >> sys.stderr, url
        raw_response = urllib2.urlopen(url)
        json_response = json.load(raw_response)
        raw_response.close()
    
        for tweet in json_response["results"]:
            id = tweet["id"]
            timestamp = calendar.timegm(rfc822.parsedate(tweet["created_at"]))
            from_user = clean_string(tweet["from_user"])
            ##from_user_id = clean_string(tweet["from_user_id"])
            text = clean_string(tweet["text"])
            iso_language_code = ""
            ##iso_language_code = tweet["iso_language_code"]
            row = str(id) + " : " + str(timestamp) + " : " + from_user + " : " + text + " : " + iso_language_code
            print row.encode('utf8')
            if db_cursor != False:
                sql_statement = "insert into %s (id, from_user, timestamp, text, iso_language_code) values (%d, '%s', %d, '%s', '%s')" %  (db_table, id, from_user,  timestamp, text.replace("'","\\'"), iso_language_code)
                db_cursor.execute(sql_statement.encode('utf8'))
                
        if "next_page" in json_response.keys():
            query = json_response["next_page"]
        else:
            max_id = json_response["max_id"]
            query = ""
            result = max_id
            
    return max_id


def rep_dump_tweets(q, since_id=0, sleep=3600, verbose=True, rpp=100, result_type = 'recent', filename=False, db_cursor=False, db_table=False):
    while True:
        if verbose:
            print >> sys.stderr, "since_id = " + str(since_id)
        max_id = dump_tweets(q=q, since_id=since_id, verbose=verbose, db_cursor=db_cursor, db_table=db_table)
        if verbose:
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
        opts, args = getopt.getopt(sys.argv[1:], "d:fhi:q:s:v", ["db:", "file", "help", "query=", "since_id=", "sleep="])
    except getopt.GetoptError, err:
        usage()
        sys.exit(2)

    q = ""
    since_id = 0  
    sleep = 0
    db = False
    filename = False
    verbose = False
    
    for o, a in opts:
        if o == "-v":
            verbose = True
        elif o in ("-d", "--database"):
            db = string.split(a,":")
        elif o in ("-f", "--file"):
            filename = True
        elif o in ("-h", "--help"):
            usage()
            sys.exit()
        elif o in ("-i", "--since_id"):
            since_id = int(a)
        elif o in ("-q", "--query"):
            q = a
        elif o in ("-s", "--sleep"):
            sleep = int(a)
        else:
            assert False, "unhandled option"

    if q == "":
        print("Required -q option is missing. No search text no party!")
        usage()
        sys.exit(3)
        
    if filename:
        filename = ".tweets_" + q
        if os.path.isfile(filename):
            if verbose:
                print >> sys.stderr, "Reading since_id from file " + filename
            since_id = id_from_file(filename)

    db_cursor = False
    db_table = False
    
    if not (db == False):
        if len(db) != 6:
            print("-d option is wrong.")
            usage()
            sys.exit(4)
            
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
                                    db = db_name)
            db_cursor = conn.cursor ()
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])
            
    
    rep_dump_tweets(q=q,
                    since_id=since_id,
                    sleep=sleep,
                    verbose=verbose,
                    filename=filename,
                    db_cursor = db_cursor,
                    db_table=db_table)
    db_cursor.close()
    conn.close()
    
if __name__ == "__main__":
    main()
