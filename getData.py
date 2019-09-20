#python 3.6
#pip install PyMySQL

import json
import pymysql
import csv

addr = "localhost"
user = "foo"
password = "password"
database = "datavis_project"

ds1_segs = [[] for y in range(8)]
ds2_segs = [[] for y in range(8)]
ds3_segs = [[] for y in range(8)]

ds1_interacts = []
ds2_interacts = []
ds3_interacts = []

#open files docs .json
with open('./PROJECT - visual summary from log data/ProvSegments/Dataset_1/Documents/Documents_Dataset_1.json') as json_file1:
   dataset_1_docs = json.load(json_file1)

with open('./PROJECT - visual summary from log data/ProvSegments/Dataset_2/Documents/Documents_Dataset_2.json') as json_file2:
   dataset_2_docs = json.load(json_file2)

with open('./PROJECT - visual summary from log data/ProvSegments/Dataset_3/Document/Documents_Dataset_3.json') as json_file3:
   dataset_3_docs = json.load(json_file3)

#read segmentation files for all segs in dataset 1 .csv
for idx in range(8):
   with open('./PROJECT - visual summary from log data/ProvSegments/Dataset_1/Segmentation/Arms_P' + str(idx+1) + '_20_4_6_Prov_Segments.csv') as seg_file1:
      csv_reader = csv.reader(seg_file1, delimiter=',')
      for row in csv_reader:
         ds1_segs[idx].append(row)

#read segmentation files for all segs in dataset 2 .csv
for idx in range(8):
   with open('./PROJECT - visual summary from log data/ProvSegments/Dataset_2/Segmentation/Terrorist_P' + str(idx+1) + '_20_4_6_Prov_Segments.csv') as seg_file2:
      csv_reader = csv.reader(seg_file2, delimiter=',')
      for row in csv_reader:
         ds2_segs[idx].append(row)

#read segmentation files for all segs in dataset 3 .csv
for idx in range(8):
   with open('./PROJECT - visual summary from log data/ProvSegments/Dataset_3/Segmentation/Disappearance_P' + str(idx+1) + ('_20_4_6_Prov_Segments.csv' if idx != 7 else '_20_3_6_Prov_Segments.csv')) as seg_file3:
      csv_reader = csv.reader(seg_file3, delimiter=',')
      for row in csv_reader:
         ds3_segs[idx].append(row)

#read interaction files for dataset 1 .json
for idx in range(8):
   with open('./PROJECT - visual summary from log data/ProvSegments/Dataset_1/User Interactions/Arms_P' + str(idx+1) +'_InteractionsLogs.json') as interactions_file1:
      ds1_interacts.append(json.load(interactions_file1))

#read interaction files for dataset 2 .json
for idx in range(8):
   with open('./PROJECT - visual summary from log data/ProvSegments/Dataset_2/User Interactions/Terrorist_P' + str(idx+1) +'_InteractionsLogs.json') as interactions_file2:
      ds2_interacts.append(json.load(interactions_file2))

#read interaction files for dataset 3 .json
for idx in range(8):
   with open('./PROJECT - visual summary from log data/ProvSegments/Dataset_3/User Interactions/Disappearance_P' + str(idx+1) + '_InteractionsLogs.json') as interactions_file3:
      ds3_interacts.append(json.load(interactions_file3))


    


# Open database connection
conn = pymysql.connect(host=addr, user=user, password=password)
cursor = conn.cursor()

# prepare a cursor object using cursor() method
cursor.execute("create database if not exists " + database)
cursor.execute("use " + database)

#create table docs
cursor.execute('create table if not exists docs (did int not null auto_increment, datasetnum int, id varchar(64), title varchar(256), date varchar(32), type varchar(32), contents text, primary key (did))')

#create table segmentation
cursor.execute('create table if not exists segmentations (did int not null auto_increment, datasetnum int, participantnum int, ID int, start float, end float, length float, primary key (did))')

#create table interactions
cursor.execute('create table if not exists interactions (did int not null auto_increment, datasetnum int, participantnum int, duration int, txt text, interactiontype varchar(64), id varchar(64),  time int, primary key(did))')



def enterDocs(dataset_num, docs_json, conn1):
   curs = conn1.cursor()
   for idx in range(len(docs_json)):
      try: 
         sql = 'INSERT INTO docs (datasetnum, id, title, date, type, contents) VALUES (%d, "%s", "%s", "%s", "%s", "%s")' \
                % (dataset_num, docs_json[idx]['id'], docs_json[idx]['title'], docs_json[idx]['date'], docs_json[idx]['type'], pymysql.escape_string(docs_json[idx]['contents']))
         curs.execute(sql)
         conn1.commit()
      except Exception as e:
          # Rollback in case there is any error
          print("error inserting docs: %s %s %s" % (dataset_num, docs_json[idx]['id'], e))
          conn1.rollback()

def enterSegs(dataset_num, seg_array, conn2):
  #for seg_array 
  #0 = id
  #1 = start
  #2 = end
  #3 = length               
   curs = conn2.cursor()
   for idx in range(len(seg_array)):
      for row in range(1, len(seg_array[idx])): #first row is headers
         try:
            sql = 'INSERT INTO segmentations (datasetnum, participantnum, ID, start, end, length) VALUES (%d, %d, %d, %f, %f, %f)' \
                  % (dataset_num, idx+1, int(seg_array[idx][row][0]), float(seg_array[idx][row][1]), float(seg_array[idx][row][2]), float(seg_array[idx][row][3]))
            curs.execute(sql)
            conn2.commit()
         except Exception as e:
            # Rollback in case there is any error
            print("error inserting segmentation- dataset: %d, participant: %d, id: %d,  %s" % (dataset_num, idx+1, int(seg_array[idx][row][0]), e))
            conn2.rollback()

def enterInteractions(dataset_num, interacts_array, conn3):
   curs = conn3.cursor()
   for idx in range(len(interacts_array)):
      for rec_num in range(len(interacts_array[idx])):
         try:
            sql = 'INSERT INTO interactions (datasetnum, participantnum, duration, txt, interactiontype, id, time) VALUES (%d, %d, %d, "%s", "%s", "%s", %d)' \
                  % (dataset_num, idx+1, interacts_array[idx][rec_num]['duration'], pymysql.escape_string(interacts_array[idx][rec_num]['Text']), interacts_array[idx][rec_num]['InteractionType'], \
                     interacts_array[idx][rec_num]['ID'], interacts_array[idx][rec_num]['time'])
            curs.execute(sql)
            conn3.commit()
         except Exception as e:
            #Rollback in case there is any error
            print("error inserting interactions- dataset: %d, participant: %d, text: %s" % (dataset_num, idx+1, interacts_array[idx][rec_num]['Text']))
            conn3.rollback()

enterDocs(1, dataset_1_docs, conn)
enterDocs(2, dataset_2_docs, conn)
enterDocs(3, dataset_3_docs, conn)

enterSegs(1, ds1_segs, conn)
enterSegs(2, ds2_segs, conn)
enterSegs(3, ds3_segs, conn)

enterInteractions(1, ds1_interacts, conn)
enterInteractions(2, ds2_interacts, conn)
enterInteractions(3, ds3_interacts, conn)


conn.close()
