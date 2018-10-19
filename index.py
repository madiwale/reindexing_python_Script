#!/usr/bin/python

######################################
# Postgres Index Maintenance Script
version = 2.1
######################################

import sys,datetime,argparse,psycopg2
from psycopg2 import extras

#Global Vars :(
sql = """SELECT n.nspname,c.relname as table_name,i.relname as index_name, x.indexrelid , i.relpages/c.relpages ::float AS iratio, i.relpages*8192::bigint idxsize ,c.relpages*8192::bigint tabsize ,
pg_get_indexdef(x.indexrelid) as indDef,r.pks,r.uks,r.fks,
x.indisprimary,x.indisunique
FROM pg_index x 
JOIN pg_class i ON i.oid = x.indexrelid AND i.relkind = 'i' 
JOIN pg_class c ON c.oid = x.indrelid AND c.relkind = 'r'
JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN (SELECT SUM(CASE WHEN contype = 'p' THEN 1 END) as pks, 
          SUM(CASE WHEN contype = 'u' THEN 1 END) as uks,
          SUM(CASE WHEN contype = 'f' THEN 1 END) as fks, connamespace,conindid FROM pg_constraint 
          GROUP BY connamespace , conindid) r 
    ON  r.conindid = x.indexrelid and r.connamespace = n.oid
where i.relpages > 128 AND c.relpages > 1 """
strtTime = datetime.datetime.now()


#Command Line Argument parser and help display
parser = argparse.ArgumentParser(description='Index Analysis and Rebuild Program',
	epilog='Example 1:\n %(prog)s -c "host=host1.hostname.com dbname=databasename user=username password=password"\n'
    'Example 2:\n %(prog)s -c "host=host1.hostname.com dbname=databasename user=username password=password"  --tsvfile=test.tsv --ddlfile=ddl.sql --errorlog=error.log --execute --quitonerror',
	formatter_class=argparse.RawTextHelpFormatter)
parser.add_argument('-c','--connection',help="Connection string containing host, username, password etc")
parser.add_argument('-i','--include_schema',help="List of schema names which need to be included")
parser.add_argument('-e','--exclude_schema',help="List of schema names which need to be excluded")
parser.add_argument('--endtime',help="Endtime in YYYY-MM-DD-HH:MM:SS format (currently ignored)")
parser.add_argument('--tsvfile',help="Generate TSV (Tab Seperated Values) file")
parser.add_argument('--ddlfile',help="Generate DDL as SQL Script")
parser.add_argument('--errorlog',help="Error log file")
parser.add_argument('--displayddl', action='store_true', help="Display Generated DDLs on the screen")
parser.add_argument('--quitonerror', action='store_true', help="Exit on execution Error")
parser.add_argument('--iratio',help="Minimum Index Ratio. above which it is considered for reindexing",default=0.9)
parser.add_argument('--execute', action='store_true',help="Execute the generated DDLs against database")
if len(sys.argv)==1:
    parser.print_help()
    sys.exit(1)

args = parser.parse_args()

#Print the version of this program to stdout
def print_version():
    print("Version: "+str(version))

#Establish connection to database and handle exception
def create_conn():
    print("Connecting to Databse...")
    try:
       conn = psycopg2.connect(args.connection+" connect_timeout=5")
    except psycopg2.Error as e:
       print("Unable to connect to database :")
       print(e)
       sys.exit(1)
    return conn

#close the connection
def close_conn(conn):
    print("Closing the connection...")
    conn.close()
    

def prepareSQL():
    global sql
    sql = sql + "AND i.relpages/c.relpages ::float > " + str(args.iratio) + " AND n.nspname "
    if args.include_schema :
        print("Include Schema is specified : " + ','.join(map("'{0}'".format,args.include_schema.split(","))))
        sql = sql + " IN (" + ','.join(map("'{0}'".format,args.include_schema.split(","))) + ")"
    elif args.exclude_schema :
        print("Exclude Schema is specified : " + ','.join(map("'{0}'".format,args.exclude_schema.split(","))))
        sql = sql + " NOT IN (" + ','.join(map("'{0}'".format,args.exclude_schema.split(","))) + ")"
    else :
        print("Default Exclude list of schema :  'pg_catalog','information_schema','pg_toast'")
        sql = sql + " NOT IN ('pg_catalog','information_schema','pg_toast')"
    print(sql)

#Get the Indexes in a Dictionary.
def getIdxDict():
    print("Quering the Database...")
    cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql)
    index_list = cur.fetchall()
    cur.close()
    print("Number of Indexes are: " + str(len(index_list)))
    return index_list

#generate DDL statements
def genStmnts(index_list):
    print("Generating DDL Statements...")
    for o in index_list:
        #print(o['uks'])
        if o['indisprimary'] == False and o['fks'] == None and o['uks'] ==None  :
            idef = o['inddef']
            dupIndx = idef[0:idef.find('INDEX')+5]+" CONCURRENTLY "+o['index_name']+"_bk"+idef[idef.find('INDEX')+6+len(o['index_name']):]
            #print(dupIndx)
            o['DDL1'] = dupIndx
            dropIdx = "DROP INDEX \"" + o['nspname'] + "\"." + o['index_name']
            #print(dropIdx)
            o['DDL2'] = dropIdx
            renIndx = "ALTER INDEX \"" + o['nspname'] + "\"." + o['index_name'] + "_bk RENAME TO " + o['index_name']
            #print(renIndx)
            o['DDL3'] = renIndx
        elif o['indisprimary'] == True and o['fks'] == None and o['uks'] ==None :
            idef = o['inddef']
            dupIndx = idef[0:idef.find('INDEX')+5]+" CONCURRENTLY "+o['index_name']+"_bk"+idef[idef.find('INDEX')+6+len(o['index_name']):]
            o['DDL1'] = dupIndx
            #print(dupIndx)
            renIndx = "ALTER TABLE \"" + o['nspname'] + "\"." + o['table_name'] + " DROP CONSTRAINT " + o['index_name'] + ", ADD CONSTRAINT " \
                + o['index_name'] + " PRIMARY KEY USING INDEX " + o['index_name'] + "_bk"
            o['DDL2'] = renIndx
        elif o['uks'] == 1 and o['fks'] == None :
            idef = o['inddef']
            dupIndx = idef[0:idef.find('INDEX')+5]+" CONCURRENTLY "+o['index_name']+"_bk"+idef[idef.find('INDEX')+6+len(o['index_name']):]
            o['DDL1'] = dupIndx
            renIndx = "ALTER TABLE \"" + o['nspname'] + "\"." + o['table_name'] + " DROP CONSTRAINT " + o['index_name'] + ", ADD CONSTRAINT " \
                + o['index_name'] + " UNIQUE USING INDEX " + o['index_name'] + "_bk"
            o['DDL2'] = renIndx


#print DDLs to terminal (stdout)
def printDDLs(index_list):
    for o in index_list:
        for i in range(1,len(o)-12):
            print(o['DDL'+str(i)])  

def writeDDLfile(index_list,ddlfile):
    fd = open(ddlfile, 'w')
    fd.truncate()
    for o in index_list:
        fd.write("----Rebuiding "+o['index_name']+" on table "+o['nspname'] + "." + o['table_name']+"-----\n")
        for i in range(1,len(o)-12):
            fd.write(o['DDL'+str(i)]+";\n")
        fd.write('\n')
    fd.close()

def writeIndexTSV(index_list,tsvfile):
    print("Generating Tab Seperated File : "+ tsvfile)
    fd1 = open(tsvfile,'w')
    for o in index_list:
        fd1.write(strtTime.strftime('%Y-%m-%d %H:%M:%S')+"\t"+o['nspname']+"."+o['table_name']+"."+o['index_name']+"\t"+str(o['iratio'])+"\t"+str(o['idxsize'])+"\n")
    fd1.close()  

def executeDDLs(index_list):
    if args.errorlog:
        fd = open(args.errorlog,'w')
    old_isolation_level = conn.isolation_level
    conn.set_isolation_level(0)
    for o in index_list:
        for i in range(1,len(o)-12):
            strDDL = o['DDL'+str(i)]
            try:
                cur = conn.cursor()
                print("Executing :" + strDDL)
                cur.execute(strDDL)
                conn.commit()
                cur.close()
            except psycopg2.Error as e:
                print("Statement Execution Error :")
                print(e)
                if args.errorlog:
                    fd.write(strDDL + str(e))
                if args.quitonerror :
                    sys.exit(1)
    conn.set_isolation_level(old_isolation_level)
    if args.errorlog:
        fd.close()

#main() function of the program
if __name__ == "__main__":
    print_version()
    conn = create_conn()
    prepareSQL()
    index_list = getIdxDict()
    genStmnts(index_list)
    
    #if user specified the --displayddl option
    if args.displayddl:
        printDDLs(index_list)
    
    if args.ddlfile:
        writeDDLfile(index_list,args.ddlfile)
    
    #if user specified the --tsvfile option
    if args.tsvfile :
        writeIndexTSV(index_list,args.tsvfile)
    
    #if user specified the --execute option
    if args.execute:
        print("Auto execute is Enabled")
        executeDDLs(index_list)
    else:
        print("Auto execute is disabled")
    
    close_conn(conn)
