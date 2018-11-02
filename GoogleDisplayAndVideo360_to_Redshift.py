#!/usr/bin/python
#****************************************************************************************
#    DBM  Python Script for  Fetching data from Doubleclick Bid Manager/ Google Display and Video  360
#   --------------------------------------------
#
#    Notes:
#
#    1. This script fetches data use DBM Google API for the XXXXXe Client.
#       You can see the variables below for the details of the API
#    2. Script take JSON as a input from the config_file
#    3. Once the data is received it writes to S3 (AWS).Please see the Variables for
#       the path details
#    4. Once Data is written to S3 it writes it to the Redshift staging table.Staging
#       table is truncated before writing the data
#    5. Data is inserted into Master table after the above step
#    6. It overrides 30 days of data. You can change it based on your requirement
#
#    Version:
#      Please check Git for Change Management & Details.
#
#    Author:
#      Harmeet Sokhi
#
#
#    Date :
#      18/10/2018
#*******************************************************************************************
import httplib2
import pprint
import simplejson
import time
import json
import psycopg2
import boto3
import boto
import os
import botocore
import datetime
import sys
import urllib2

from contextlib import closing

from googleapiclient.errors import HttpError
from apiclient.discovery import build
from apiclient.http import MediaFileUpload
from oauth2client.client import OAuth2WebServerFlow
from oauth2client.client import OAuth2Credentials
from oauth2client import GOOGLE_AUTH_URI

from oauth2client import GOOGLE_REVOKE_URI
from oauth2client import GOOGLE_TOKEN_URI



### Jenkins Passowrds  

REFRESH_TOKEN = os.environ['REFRESH_TOKEN']
CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_SECRET = os.environ['CLIENT_SECRET']
PWD = os.environ['Client_Admin_DBPWD'] 

# Progaram Variables
API_NAME = "doubleclickbidmanager"
API_VERSION = "v1"
REDIRECT_URI = 'urn:ietf:wg:oauth:2.0:oob'
Config_File = 'config.json.dbm.gen'   ## 
DBNAME = 'Client'
HOST = 'Redshift database PATH'
PORT = 5439
USER = 'admin_Client'
SCHEMA = 'digital'
Staging_Table  = 'Redshift_Table_Stg'
Master_Table =  'Redshift_Table_Master'
AWS_IAM_ROLE = "RedhsiftxxxxRolewith S3 Access"
S3_FilePath = "ClientOnline/DBM/General/"
EC2_File_Path = "Intermediate Server Path to write temporary files"
reportName = 'Report Name'
OAUTH_SCOPE = ['https://www.googleapis.com/auth/doubleclickbidmanager']

def authorization():
	try:
	        credentials = OAuth2Credentials(None, CLIENT_ID, CLIENT_SECRET, REFRESH_TOKEN, None,
        	                               GOOGLE_TOKEN_URI, None,
                	                       revoke_uri=GOOGLE_REVOKE_URI,
                        	               id_token=None,
                                	       token_response=None)

	        # Create an httplib2.Http object and authorize it with  credentials
	        http = httplib2.Http()
	        http = credentials.authorize(http)
		return http	
	except Exception as e:
		pprint.pprint(" >> Error in authorization: %s" %e)
		sys.exit(1)
	
def gen_report(configfile,http):
        with open(configfile,'r') as f:
             bodystr = f.read()
        bodyvar = bodystr % (startDATE_epochms,endDATE_epochms)

	print ' >> Json: ' + bodyvar
        body = simplejson.loads(bodyvar)
        SERVICE = build(API_NAME, API_VERSION, http=http)

	print ' >> Creating the Json query'
        request = SERVICE.queries().createquery(body=body)
	print ' >> Request created'

        json_data = request.execute()
	print ' >> Request executed'

	print ' >> Report generated. Query ID ' + json_data['queryId']
        return [json_data['queryId'], SERVICE]


def fetchreport(SERVICE,report_id,con):
	global reportName
	#Generate and print sample report.
	
	#  Args:
	#    service: An authorized Doublelcicksearch service.
	#    report_id: The ID DS has assigned to a report.
	#    report_fragment: The 0-based index of the file fragment from the files array.

	reportNameTemp = reportName
	if startDATE == endDATE:
		reportName = reportNameTemp + '_' + str(startDATE) + '_' + '.csv'
		reportName_x = reportNameTemp + '_x_' + str(startDATE) + '_' + '.csv'
		file_in_s3 = S3_FilePath + str(startDATE) + '/' + reportName 
	else:
		reportName = reportNameTemp + '_' + str(startDATE) + '_' + str(endDATE) + '.csv'
                reportName_x = reportNameTemp + '_x_' + str(startDATE) + '_' + str(endDATE) + '.csv'
		file_in_s3 = S3_FilePath + str(startDATE) + '...' + str(endDATE) + '/' + reportName

	repfile = EC2_File_Path + reportName
	repfile_x = EC2_File_Path + reportName_x
	for _ in xrange(50):
	   try:
	      request = SERVICE.queries().getquery(queryId=report_id)
	      query = request.execute()
              report_url = query['metadata']['googleCloudStoragePathForLatestReport']

	      if report_url <> "":
	        	print ' >> The report is ready.'
		        # Grab the report and write contents to a file.
		        print ' >> ' + report_url
	        	with open(repfile, 'wb') as output:
	        	  with closing(urllib2.urlopen(report_url)) as url:
		            output.write(url.read())
        		print ' >> Download complete.'
			
			#Just write the individual records and remove other information 
			counter = 0
			with open(repfile) as f:
			 	with open(repfile_x, "w") as f1:
			        	for line in f:
					    if ",,,,," in line or line.strip() == '':	
						break	
					    else:
                                                f1.write(line)
                                                counter = counter + 1
		

		        f.close()
			f1.close()

        		print ' >> Filename in EC2 : ' + repfile
		        print ' >> Total records in File: ' + str(counter)

			# Grab the report and write contents to a file.
		        FilePath = write_to_s3(repfile_x,"bucketname",file_in_s3)
		        print ' >> File written in S3. File Path is : ' + FilePath
			write_to_db(FilePath,con)
  		        return
	      else:
              	  print ' >> Report is not ready. I am trying again...'
	      	  time.sleep(30)
	   except HttpError as e:
      		error = simplejson.loads(e.content)['error']['errors'][0]
		print ' >> ' + error
		break

def  write_to_s3(file_to_upload,bucket,file_name_in_s3):
	try:
		s3 = boto3.resource('s3')
		s3.Bucket(bucket).upload_file(file_to_upload,file_name_in_s3)
		return "s3://bucketname/" + file_name_in_s3 
	except Exception as e:
		pprint.pprint(' >> Unable to upload to S3 : %s' %e)
		sys.exit(1)
		
def  write_to_db(FILE_PATH,con):
  try:
        deletesql = " Truncate Table " + SCHEMA + "." + Staging_Table
        print " >> " + deletesql
        cur = con.cursor()          
        # drop previous days contents from the staging table
        try:
	    print(" >> Truncating the staging table......")	
            cur.execute(deletesql)
            print(" >> Truncate for staging executed successfully")

        except Exception as e:
            pprint(" >> Error executing truncate in the staging table: %s " % e)
            type_, value_, traceback_ = sys.exc_info()
            pprint(traceback.format_tb(traceback_))
            pprint(type_, value_)
            sys.exit(1)

        # Copy to the staging table first and if it is successful copy to the master table

        copysql = """copy {}.{} from '{}' credentials 'aws_iam_role={}' format as csv IGNOREHEADER 1""".format(SCHEMA,Staging_Table,FILE_PATH,AWS_IAM_ROLE)
        print " >> " + copysql
        try:
            cur.execute(copysql)
            print(" >>> Copy Command executed successfully !")
            # query to display number of rows that are to be deleted from the master table
            select_deleted = """
                        Select * from {}.{} 
                        where {}.{}.date in (select distinct(date) from {}.{})
                        """.format(SCHEMA, Master_Table, SCHEMA, Master_Table,
				   SCHEMA, Staging_Table)
            # query to display total number of rows that are to be ingested in the master table
            select_staging = """Select * from {}.{}""".format(SCHEMA, Staging_Table)

            # Copy to the Master Table
            sqlMaster = """
                                    begin transaction;
                                    delete from {}.{}
                                    using {}.{}
                                    where 
                                    {}.{}.date = {}.{}.date;
                                    insert into {}.{}
                                    select * from {}.{};
                                    end transaction;
                                    """.format(SCHEMA, Master_Table,
                                               SCHEMA, Staging_Table,
                                               SCHEMA, Master_Table, SCHEMA, Staging_Table,
                                               SCHEMA, Master_Table,
                                               SCHEMA, Staging_Table)
            try:
                print ' >> Running select delete'
		print select_deleted
                cur.execute(select_deleted)
		print ' >> select_delete done'
                #delete_count = cur.rowcount
                print ' >> Number of rows to be deleted in Master: ' + str(cur.rowcount)
		#print '>>> delete_count: ' + delete_count
                print ' >> ' + select_staging
                cur.execute(select_staging)
	        print ' >> staging select done'
                #staging_count = cur.rowcount
                #print '>>> staging_count: ' + staging_count
                print ' >> Number of rows in the staging table: ' + str(cur.rowcount)
                print sqlMaster
                cur.execute(sqlMaster)
                con.commit()
                #print(">> Number of Rows Deleted: {}".format(str(delete_count)))
                #print(">> Number of Rows Inserted : {}".format(str(staging_count)))
                print(">> Data Ingestion Successfull !")

            except Exception as e:
                pprint(" >> Failed to execute copy command. Error : %s" % e)
                type_, value_, traceback_ = sys.exc_info()
                pprint(traceback.format_tb(traceback_))
                print(type_, value_)
                sys.exit(1)

        except Exception as e:
            pprint(' >> Unable to connect to Redshift.Error: %s' % e)
            type_, value_, traceback_ = sys.exc_info()
            pprint(traceback.format_tb(traceback_))
            print(type_, value_)
            sys.exit(1)

  except Exception as e:
        pprint(' >> Ingestion Operation Failed with Error:::::: %s' % e)
        type_, value_, traceback_ = sys.exc_info()
        pprint(traceback.format_tb(traceback_))
        print(type_, value_)
        sys.exit(1)


def getDates(con):

	        startDATE = os.getenv('startDATE',"")
        	endDATE = os.getenv('endDATE',"")

		if startDATE == "":
			print ' >> Calculating StartDATE '
			cur = con.cursor()
		        sqlMaster = " select max(Date) from " + SCHEMA + "." + Master_Table 
		        try:
		            cur.execute(sqlMaster)
			    print ' >> Executed the max Date query '
             		    sqlRow = cur.fetchone()
			    print ' >> Fetching the date row,if any '
	                    if sqlRow[0] == None:
			    	startDATE = ""
			    else:
				startDATE = sqlRow[0]
			    if startDATE == "":
				print ' >> No max Date row '
				yesterDate = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')
				startDATE = yesterDate
			    else:
				print ' >> setting the startDATE to next day of last updated data'
				startDATE = startDATE - datetime.timedelta(days = 30)
#	           	    print ' >> startDate is: ' + startDATE
		        except Exception as e:
		            pprint.pprint(" >> Failed to get Max Date from the Master Table %s " %e)
			    sys.exit(1)
		else:
			print ' >> Settiing startDATE from Jenkins parm'
			startDATE = os.environ['startDATE']			
			
		if endDATE == "":
			print ' >> Going to calculate endDate'
			yesterDate = (datetime.datetime.now() - datetime.timedelta(1)).strftime('%Y-%m-%d')			
			endDATE = yesterDate
		else:
			print ' >> Setting the endDATE from Jenkins parm'
			endDATE = os.environ['endDATE']

		startDATE = str(startDATE)
		endDATE = str(endDATE)

		if startDATE > endDATE:
			print ' >> startDATE greater than endDATE ?? I am setting both as endDATE'
			startDATE = endDATE
		
		print ' >> startDate : ' + startDATE 	
		print ' >> endDate: ' + endDATE
		 
		return [startDATE,endDATE]


def connectDB():
        print(" >> Establishing Connection with Redshift..")
        con=psycopg2.connect(dbname= DBNAME, host=HOST, port= PORT, user= USER, password= PWD)
        print(" >> Connection Successful!")
	return con

def main():	
	
	global startDATE 
	global endDATE
	global startDATE_epochms
	global endDATE_epochms

	# Get the startDATE and endDATE values from the environment Varibales in jenkins
	startDATE = os.getenv('startDATE',"")
	endDATE = os.getenv('endDATE',"")
	#startDATE = '2018-01-01'
        #endDATE = '2018-01-01'
	try:
		# Authorize thru the Access Token
		print ' >> Starting API authorization'
		http = authorization()
		print ' >> Authorization Successful'        					
		# Connect to the redshift Database
		con = connectDB()
		print ' >> Setting Dates '
		# if the dates are not passed as parms  need to calculated 
	   	startDATE, endDATE = getDates(con)		
		
		# Convert Dates to epochms as DBM API takes long Date as input
		d = datetime.datetime.strptime(startDATE + ' 12:00:00,00',
                           '%Y-%m-%d %H:%M:%S,%f').strftime('%s')
		startDATE_epochms = int(d)*1000
		print ' >> startDATE epochms: ' + str(startDATE_epochms)
		print ' >> ' + str(datetime.datetime.fromtimestamp(float(d)))
                d = datetime.datetime.strptime(endDATE + ' 23:59:59,59',
                           '%Y-%m-%d %H:%M:%S,%f').strftime('%s')
		endDATE_epochms = int(d)*1000
		print ' >> endDATE epochms: ' + str(endDATE_epochms)
                print ' >> ' + str(datetime.datetime.fromtimestamp(float(d)))

		# Create the query and get the report ID
		print ' >> Setting JSON and generating report '
		report_id, SERVICE = gen_report(Config_File,http)

		# Fetch the report using the id and upload in S3 and redshift
		print ' >> Fetching Report '
		fetchreport(SERVICE, report_id,con)

	except Exception as e:
                pprint.pprint(" >> Error in the script : %s" %e)
		sys.exit(1)
			
	
if __name__ == "__main__":				
	main()