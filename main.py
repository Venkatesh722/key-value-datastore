import pandas as pd
import portalocker
import os.path
import os
import csv
import sys
import json
import datetime as dt
from platform import system
from os import path
from datetime import datetime,timedelta
from settings import FILENAME,KEY_LIMIT,VALUE_LIMIT,SIZE_LIMIT
FIELDS = ['Key','Value','Expiration_time']



#---------------------------------------------------------------------------------------------------------------------------------------------------
'''Function creates the directory if not mentioned in cmd line arguments .Reads the file if the file available in directory if not, creates the
the file named datastore.csv and locks the file while writing such that no other process access the file datastore.csv and unlocks after writing data 
to it.'''

def create_file():
	operating_system = system()

	try:
		path_to_file = sys.argv[1]		
	except:
		if(operating_system == 'Windows'):
			path_to_file = 'C:/Datastore/key-value/'
			if(path.exists(path_to_file)!=True):
				os.mkdir(path_to_file)
		elif(operating_system == 'Linux'):
			path_to_file = '/root/Datastore/key-value/'
			if(path.exists(path_to_file)!=True):
				os.mkdir(path_to_file)
	absolute_path = path_to_file+FILENAME
	if(path.exists(absolute_path)!=True):
		try:
			with open(absolute_path,'w') as csvfile:
				portalocker.lock(csvfile, portalocker.LOCK_EX)
				csvwriter = csv.writer(csvfile)
				csvwriter.writerow(FIELDS)
				portalocker.unlock(csvfile)
		except:
			print(json.dumps({'status':'failed','message':'Error in opening file'}))
			quit()
	return absolute_path

#---------------------------------------------------------------------------------------------------------------------------------------------------
#Function that displays menu and returns the user's choice

def display_menu():
	print('\n------------------------MENU-------------------------------')
	print('1. Create\n2. Read\n3. Delete\n4. Exit')
	print('-----------------------------------------------------------')
	print('Enter your choice[1-4]: ',end='')
	try:
		usr_choice = int(input())
	except:
		return(json.dumps({'status':'failed','message':'invalid data-type supplied'}))
	return usr_choice

#---------------------------------------------------------------------------------------------------------------------------------------------------
'''Fucntion that reads csv file and takes to the dictionary to achive O(1) time complexity to check whether the key exists or not in datastore.The modes 
R-Read_key returns user key , D-Delete returns the expiration time,W-Write return true if key available else ready for creation of a new key.'''

def key_exists(usr_key,mode):
	data = pd.read_csv(absolute_path)
	key_value_dict = pd.Series(data.Value.values,index=data.Key).to_dict()
	key_expiration_dict = pd.Series(data.Expiration_time.values,index=data.Key).to_dict()
	if(mode == 'W'):
		if usr_key in key_value_dict.keys():
			return True
		else:
			return False
	elif(mode == 'R'):
		return key_value_dict[usr_key]
	elif(mode == 'D'):
		return key_expiration_dict[usr_key]

#---------------------------------------------------------------------------------------------------------------------------------------------------
'''Function that creates the key-value with expiration time if it satisfies the constraints that we have.Locks the file when writing and release
once completed '''

def Create():
	time_now = datetime.now()
	file_stats = os.stat(absolute_path)
	size = file_stats.st_size
	data = pd.read_csv(absolute_path)

	if(size > SIZE_LIMIT):
		return(json.dumps({'status':'failed','message':'file size exceeded'}))
	key = input("Enter key: ")
	if(len(key) > KEY_LIMIT):
		return(json.dumps({'status':'failed','message':'exceeds key size limit'}))
	if(key_exists(key,'W')):
		return(json.dumps({'status':'failed','message':'key already exists'}))
	value = input("Enter value: ")
	if(len(value) > VALUE_LIMIT):
		return(json.dumps({'status':'failed','message':'exceeds value size limit'}))
	try:
		ttl = int(input('Enter Time-To-Live :(Enter 0 if not required): '))
		print("\n")
	except:
		return(json.dumps({'status':'failed','message':'invalid data-type supplied'}))
	if(ttl > 0):
		Expiration_time = time_now + timedelta(seconds=ttl)
	elif(ttl == 0):
		Expiration_time = 'None'
	else:
		return(json.dumps({'status':'failed','message':'ttl cannot be negative'}))
	value = value.replace('"','')
	new_entry = {'Key':key,'Value':value,'Expiration_time':Expiration_time}
	data1 = data.append(new_entry,ignore_index=True)
	lock = portalocker.Lock(absolute_path, mode='a+b', flags=portalocker.LOCK_EX)
	data1.to_csv(absolute_path,index=False)
	lock.acquire()
	lock.release()
	return (json.dumps({'status':'OK','message':'Inserted Successfully'}))

#---------------------------------------------------------------------------------------------------------------------------------------------------
'''Function that takes the key and checks if it is available in the datastore.If key exists it returns the json object of key and value
 and also check if it is expired'''

def Read():
	key = input("Enter key: ")
	print("\n")
	if(key_exists(key,'W')):
		Expiration_time = key_exists(key,'D')
		time_now = datetime.now()
		if(Expiration_time == 'None'):
			return(json.dumps({'Key':key,'Value':key_exists(key,'R')}))
		if(Expiration_time != 'None'):
			Expiration_time = dt.datetime.strptime(Expiration_time, '%Y-%m-%d %H:%M:%S.%f')
			if(Expiration_time > time_now):
				return(json.dumps({'Key':key,'Value':key_exists(key,'R')}))
			else:
				Drop_row(key)
				return(json.dumps({'status':'OK','message':'The key has expired'}))
	return(json.dumps({'status':'failed','message':'key does not exist'}))

#---------------------------------------------------------------------------------------------------------------------------------------------------
'''Function that checks whether the key is present or not and deletes in the datastore if it is available and not expired'''

def Delete():
	key = input("Enter key: ")
	if(key_exists(key,'W')!=True):
		return(json.dumps({'status':'failed','message':'key does not exist'}))

	Expiration_time = key_exists(key,'D')
	time_now = datetime.now()
	data = pd.read_csv(absolute_path)

	if(Expiration_time == 'None'):
		Drop_row(key)
		return (json.dumps({'status':'OK','message':'Deleted Successfully'}))
	if(Expiration_time != 'None'):
		Expiration_time = dt.datetime.strptime(Expiration_time, '%Y-%m-%d %H:%M:%S.%f')
		if(Expiration_time > time_now):
			Drop_row(key)
			return (json.dumps({'status':'OK','message':'Deleted Successfully'}))
		else:
			Drop_row(key)
			return (json.dumps({'status':'OK','message':'The key has expired'}))

#Fucntion that drops the data and saves it to the file.And also locks the file when writing
def Drop_row(key):
	data = pd.read_csv(absolute_path)
	lock = portalocker.Lock(absolute_path, mode='a+b', flags=portalocker.LOCK_EX)
	data.drop(data[data['Key'] == key].index,inplace = True)
	data.to_csv(absolute_path,index=False)
	lock.acquire()
	lock.release()

#---------------------------------------------------------------------------------------------------------------------------------------------------
absolute_path = create_file()

while(True):
	usr_choice = display_menu()
	if(usr_choice == 1):
		print(Create())
	elif(usr_choice == 2):
		print(Read())
	elif(usr_choice == 3):
		print(Delete())
	elif(usr_choice == 4):
		break
	else:
		print(json.dumps({'status':'OK','message':'Invalid data supplied'}))