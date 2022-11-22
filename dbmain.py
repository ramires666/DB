#%% imports
import sqlite3
import os
import pickle
import re
import requests
import json
from datetime import datetime as dt
import sys
sys.path.append('/home/user/PYTHON/Projects/DSM/venv/')
from TIME_OMNI import *
from authorize import *
import pandas as pd
import openpyxl
import asyncio
import aiohttp
import time

def Date2Onix(year, month, day, hour=12, minute=0, second = 0):  # date to milliseconds
    if hour == 24:
        hour = 23
        minute = 59
        second = 59
    date = time.mktime( dt(
        int( year ),
        int( month ),
        int( day ),
        int( hour ),
        int( minute ),
        int( second )
    ).timetuple() )
    return str(int(date*1000))


def OMNI2CarName(vehicleOMNI):
    df = pd.read_excel(r'/home/user/PYTHON/Projects/DSM/venv/_lists/listAUTO_fullList.xlsx')
    try:
        carName = df[df['omniIDxl'] == int(vehicleOMNI)]['name'].values
    except:
        print(vehicleOMNI)

    return carName[0]


def get_LOG_page(_TimeFrom, _TimeTo, _vehicleID, page=1, rows=500, vehicleName='', action="getReportData", useSaved=True ):   # Onix time !
    # page = 1
    # rows = 500
    # vehicleName = ''
    # action = "getReportData"
    # useSaved = True
    # _vehicleID = 1219001271
    # _TimeFrom = Date2Onix( '2022', '07', '06', '07' )
    # _TimeTo = Date2Onix( '2022', '07', '06', '19', )

    # auth_ = {'jwt': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MTYwNTIxNDEsImxvZ2luIjoib2FvZHNtYXBpIiwidXVpZCI6ImUwNmUwOWQ1LWFmZDEtM2Q5Zi04NzI1LWRiZTRiMzljNDRlNCIsImF1dG9jaGVja19pZCI6MTAxOTU0NywicGVybWlzc2lvbnMiOlsiI1VzZXIiLCIjdXNlcl9hZGQiLCJhc2UucmVwb3J0cy5ldmVudHMiLCJhc2UucmVwb3J0cy5ncm91cHN0YXQiLCJhc2UucmVwb3J0cy5kcmQiLCJhc2UucmVwb3J0cy5tYXJjaHJvdXRlLnJvdXRlcmVwb3J0IiwiYXNlLnJlcG9ydHMuZHJpdmVyc3JlcG9ydCIsImFzZS5yZXBvcnRzLmdlb3pvbmVzcmVwb3J0IiwiYXNlLnJlcG9ydHMuZnVlbGV2ZW50c3JlcG9ydCIsImFzZS5yZXBvcnRzLmdyb3VwZXZlbnRzIiwiYXNlLnJlcG9ydHMuZ3JvdXB3b3JrMiIsImFzZS5yZXBvcnRzLnNoaWZ0cyIsImFzZS5yZXBvcnRzLm1vdmVtZW50YnlzdGFuZHJlcG9ydCIsImFzZS5yZXBvcnRzLm1hcmNocm91dGUuY3VycmVudHJ1bnMiLCJhc2UucmVwb3J0cy5sb2ciLCJhc2UucmVwb3J0cy5kZWxpdmVyeSIsImFzZS5yZXBvcnRzLnBob3RvcmVwb3J0IiwiYXNlLnJlcG9ydHMubWFwMiIsImFzZS5yZXBvcnRzLmxvY2F0aW9uIiwiYXNlLnJlcG9ydHMuZnVlbGxldmVscyIsImFzZS5yZXBvcnRzLndvcmtlZG91dGhvdXJzIiwiYXNlLnJlcG9ydHMuZW5naW5lcnBtIiwiYXNlLnJlcG9ydHMuc3BlZWQiLCJhc2UucmVwb3J0cy52b2x0YWdlIiwiYXNlLnJlcG9ydHMudW5pdmVyc2FsIiwiYXNlLnJlcG9ydHMubW92ZW1lbnRieXBlcmlvZCIsImFzZS5yZXBvcnRzLndvcmtieXRpbWUiLCJhc2UucmVwb3J0cy5sb2FkYnl0aW1lIiwiYXNlLnJlcG9ydHMubW92ZW1lbnRkaXN0cmlidXRpb24iLCJhc2UucmVwb3J0cy53b3JrZGlzdHJpYnV0aW9uYnl0aW1lIiwiYXNlLnJlcG9ydHMubG9hZGRpc3RyaWJ1dGlvbiIsImFzZS5yZXBvcnRzLmdyb3VwcmF0aW5ncyIsImFzZS5yZXBvcnRzLmNvbnNvbGlkYXRlZHJlcG9ydCIsImFzZS5yZXBvcnRzLm1vdmVtZW50YnR3c3RhbmRyZXBvcnQiLCJhc2UucmVwb3J0cy50aXJlcHJlc3N1cmUiLCJhc2UucmVwb3J0dmlzaWJpbGl0eSIsImFzZS51c2VycmVwb3J0c2NvbnRyb2wiLCJhc2UucmVmcmlnZXJhdG9yIiwiYXNlLmZ1ZWxjYXJkIiwiYXNlLnNhZmVkcml2aW5nIiwiYXNlLnRwbXMiLCJhc2UubW9kYnVzZ2VuIiwiYXNlLmZvcmVpZ25zZW5zb3Jjb250cm9sIiwic2VydmljZS5mdWVsYmFsYW5jZSIsInNlcnZpY2UuaGZtcyIsImFzZS5mdWVsbWFzcyIsInNlcnZpY2UuYmlsbGluZy51c2VyLmVkaXRvciIsImFncm8uYWNjZXNzIiwiYXNlLnJlcG9ydHMuZnVlbGV2ZW50c3JlcG9ydC5tYW51YWwiLCJhc2UucmVwb3J0cy5jb25zb2xpZGF0ZWRyZXBvcnQubWFudWFsIiwiYXNlLnJlcG9ydHMucmVmcmlnZXJhdG9yc3RhdGUiLCJzZXJ2aWNlLnJlcG9ydHMuZnVlbGJhbGFuY2UiLCJzZXJ2aWNlLnJlcG9ydHMuZnVlbHNoZWV0Iiwic2VydmljZS5zYWZlZHJpdmluZ3JlcG9ydCIsImFzZS5yZXBvcnRzLnBlcmlvZGljc2VydmljZSIsImFzZS5yZXBvcnRzLmxvY2F0aW9ucmVwb3J0IiwiYXNlLnJlcG9ydHMucmVmcmlnZXJhdG9yd29yayIsImFzZS5ncm91cHMuZ2Vvem9uZS5mdWxsIiwiYXNlLmdyb3Vwcy52ZWhpY2xlLmN1c3RvbSIsImFzZS5ncm91cHMudmVoaWNsZS52aWV3IiwiYXNlLmdyb3Vwcy52ZWhpY2xlLmVkaXR0cmVlIiwiYXNlLmdyb3Vwcy52ZWhpY2xlLnZpZXdwcm9maWxlIiwiYXNlLmdyb3Vwcy52ZWhpY2xlLmVkaXRpbmZvIiwiYXNlLmdyb3Vwcy52ZWhpY2xlLmVkaXRwcm9maWxlIiwiYXNlLmdyb3Vwcy52ZWhpY2xlLnNlcnZpY2VtbmdtbnQiLCJtYXBzLnlhbmRleC5tYXAiLCJtYXBzLndpa2kubWFwIiwibWFwcy5vc20ubWFwIiwibWFwcy5vbW5pY29tbS5tYXAiLCJtYXBzLm9tbmljb21tLmphbXMiLCJhdXRoLmNsaWNrcmVwb3J0cyIsInB1YmxpY3JlcG9ydGxpbmsuY3JlYXRlIiwiYXNlLmdyb3Vwcy5yb3V0ZS5mdWxsIiwiaWRlbnRpZmljYXRpb24iLCJtYWlucGFnZS5uZXciLCJtYXBzLmdvb2dsZS5tYXAiLCJtYXBzLmdvb2dsZS5zYXRlbGl0ZSIsIm1hcHMuZ29vZ2xlLmh5YnJpZCIsImF1dG9jaGVjay5hY2Nlc3MiLCJzZXJ2aWNlLmJpbGxpbmcubmV3IiwibWFwcy53aWtpLm1hcCIsIm1hcHMub3NtLm1hcCIsIm1hcHMuc3B1dG5pay5tYXAiLCJzZXJ2aWNlLmNvcHMucmVhZCIsInNlcnZpY2UuY29wcy51cGRhdGUiLCJzZXJ2aWNlLmNvcHMuYWRkIiwic2VydmljZS5DQU5fYnlfZW1kZCIsInNlcnZpY2Uub3ZtcyIsInNlcnZpY2UuaGZtcy5tZXNzYWdlcyIsInNlcnZpY2UucmVwb3J0cy5oZm1zX3N0YXR1cyIsInNlcnZpY2UucmVwb3J0cy5oZm1zX21lc3NhZ2VzIiwidXNlcnMubW9iaWxlLnB1c2giLCJzZXJ2aWNlLmNvcHMucmVhZCIsInNlcnZpY2UuY29wcy51cGRhdGUiLCJzZXJ2aWNlLmNvcHMuYWRkIiwic2VydmljZS5iaWxsaW5nLnVzZXIuZWRpdG9yIiwic2VydmljZS5zYWZlZHJpdmluZ3JlcG9ydCIsInNlcnZpY2UuYmlsbGluZy5uZXciLCJhc2UuZnVlbG1hc3MiLCJmdWVsbWFzcyIsImZ1ZWwuYmFsYW5jZS5uZXciLCJmdWVsLmJhbGFuY2UuZmVzX2FwaSIsImxsczUuYWNjZXNzIiwiYXV0aC5jbGlja3JlcG9ydHMiLCJzZXJ2aWNlLkNBTl9ieV9lbWRkIl0sImRyaXZlcmdyb3VwX2lkIjpudWxsLCJ2ZWhpY2xlZ3JvdXBfaWQiOiJlOThmMDgzYS01NjgwLTM2NjQtOTcyMC0wZDZiNDc1OTZiYTkiLCJ1c2VyZ3JvdXBfaWQiOm51bGwsImdlb3pvbmVncm91cF9pZCI6IjJmMTNiNzFkLWY3ZmEtMzg4OS04ZjdiLTE5NGIyNzcxMTlhMSIsInJvdXRlZ3JvdXBfaWQiOiJjOTkxNjcwYy0yNmU0LTNhNDItYWNiYi1iMTYwYTFhZGUwZDMiLCJzZXJ2ZXJfbmFtZSI6Im1pZGdhcmQiLCJtYXhfcmVwb3J0X3BlcmlvZCI6NjAsInNlcnZlciI6eyJpZCI6NzYsImhvc3QiOiJodHRwOi8vMTAuNTUuNi4xNDciLCJwb3J0Ijo4MDgwLCJzbHVnIjoibWlkZ2FyZCIsImZxZG4iOiIxMC41NS42LjE0NyIsInZvbHVtZV91bml0cyI6IkwiLCJsaWdodCI6MH0sInJvbGVzIjpbeyJpZCI6NCwibmFtZSI6IlVzZXIiLCJzbHVnIjoidXNlciIsIndlaWdodCI6MTB9LHsiaWQiOjY4LCJuYW1lIjoidXNlcl9hZGQiLCJzbHVnIjoidXNlcl9hZGQiLCJ3ZWlnaHQiOjB9XSwid2wiOnt9LCJpYXQiOjE2NTcwOTM5OTQsImV4cCI6MTY1NzA5NzU5NCwiYXVkIjoiYXNlIn0.lDDMgCBeJPt4z39Nzf_g46yF8sPYUGKoC4vpdtZlSNE', 'refresh': 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpZCI6MTYwNTIxNDEsInNpZCI6ImIyYzJhNDM0LTg4NWEtNDIwOS04YTZkLTU2MjRmZTdjYmM5NyIsInBlcm1pc3Npb25zIjpbImF1dGgucmVmcmVzaCJdLCJpYXQiOjE2NTcwOTM5OTQsImV4cCI6MTY1NzE4MDM5NH0.mGzz0DO_U9m9g-bWNem7nR9YnEN6bQPnygKNpZ9HgTg'}
    # JWT = auth_['jwt']
    JWT = auth()['jwt']

    Headers = {'accept': 'application/json',
               'Authorization': 'JWT ' + JWT,
               'Content-Type': 'application/json'}

    Params = '''{
                "params":{
                        "from":_TimeFrom,
                        "to":_TimeTo,
                        "params":{
                                "winterOffset":180,
                                "summerOffset":180,
                                "action":"getReportData",
                                "newui":true,
                                "userID":1005575,
                                "repConfigId":12273,
                                "locale":"ru",
                                "reportFromdate":_TimeFrom,
                                "fromDatetime":_TimeFrom,
                                "reportTodate":_TimeTo,
                                "toDatetime":_TimeTo,
                                "selectedRoots":["FAS"],
                                "ID":[_vehicle],
                                "vehicleID":[_vehicle],
                                "tz":"Europe/Moscow",
                                "objectType":["FAS"],
                                "objectClass":[1],
                                "service":false,
                                "rows":rowsCount,
                                "page":pageNumber,
                                "sidx":"eventdate",
                                "sord":"asc",
                                "showAddress":false
                                },
                        "url":"log",
                        "method":"POST",
                        "traditional":true
                        },
                "sync":1,
                "rebuild":true,
                "type":"ASEReport"       
                }'''


    Params = re.sub( '_TimeFrom', str(_TimeFrom), Params )
    Params = re.sub( '_TimeTo', str(_TimeTo), Params )
    Params = re.sub( '_vehicle', str(_vehicleID), Params )
    Params = re.sub( 'pageNumber', str(page), Params )
    Params = re.sub( 'rowsCount', str( rows ), Params )

    Params = re.sub( "\n", "", Params )
    Params = Params.replace( ' ', '' )

    server_answer = requests.post('https://online.omnicomm.ru/service/reports/', data=Params, headers=Headers)
    waitTime = 0.3
    while server_answer.status_code != 200:
        print(f'waiting {waitTime} seconds for correct server response {server_answer.status_code}')
        time.sleep(waitTime)
        server_answer = requests.post( 'https://online.omnicomm.ru/service/reports/', data=Params, headers=Headers )
        waitTime+=0.3

    print('Requested log page №',page,'(rows =',rows,") ",vehicleName,server_answer,"->",server_answer.elapsed)

    DATA = json.loads( server_answer.text )  # dict

    total_pages = DATA['results']['total']
    total_records = DATA['results']['records']
    current_page = DATA['results']['page']
    pagelog = DATA['results']['rows']

    # movement_stats = DATA['results']['data']
    # saving scraped MovementAndStops:
    # with open( cashed_file, 'wb' ) as f:
    #     pickle.dump( MovementAndStops, f )

    return pagelog,total_pages,total_records,current_page


def logPageDownloader(_TimeFrom, _TimeTo, _vehicleID, pageN, rows, useSaved=True, vehicleName='car'):
    total_pages = 0
    total_records = 0
    current_page = 0
    success = False
    while success == False:
        try:
            print(f'loading page# {pageN} on car={vehicleName} from=> {Onix2Date(_TimeFrom)} to=> {Onix2Date(_TimeTo)}')
            currentPageLog, total_pages, total_records, current_page = get_LOG_page(_TimeFrom, _TimeTo, _vehicleID, pageN,
                                                                                    rows, useSaved=useSaved,
                                                                                    vehicleName=vehicleName)
        except:
            success = False
            print(f'>> logPageDownloader ERR car={vehicleName} page# {pageN}')
        else:
            success = True
    return currentPageLog, total_pages, total_records, current_page



def get_LOG(_TimeFrom, _TimeTo, _vehicleID, rows=500, action="getReportData", useSaved=True ,_vehicleName=''):
    print('LOG')
    dateDate = Onix2Date(_TimeFrom).date()
    projectDir=r'/home/user/PYTHON/Projects/DSM/venv'
    dirName = f"{projectDir}/Cashed_requests/_LOGS/{dateDate}"
    if not os.path.exists(dirName): os.mkdir(dirName)
    cashed_file = f"{projectDir}/Cashed_requests/_LOGS/{dateDate}/log-{_TimeFrom}_{_TimeTo}_{_vehicleID}.pydata"
    if useSaved and os.path.exists(cashed_file):
        with open( cashed_file, 'rb' ) as pickleRick:
            log = pickle.load( pickleRick )
            return log
    else:
        print( f"LOG:{Onix2Date( _TimeFrom )}->{Onix2Date( _TimeTo )} {OMNI2CarName( _vehicleID )}" )
        list2return = []
        total_pages = 0
        total_records = 0
        current_page = 0
        currentPageLog, total_pages, total_records, current_page = logPageDownloader(_TimeFrom, _TimeTo, _vehicleID, 1, rows, useSaved=useSaved, vehicleName=_vehicleName)
        list2return.extend(currentPageLog)
        print(f"NEED from {Onix2Date(_TimeFrom)} to {Onix2Date(_TimeTo)} Total pages = {total_pages} Total records = {total_records}")

        if total_pages > 1:
            for pageNumber in range(2,total_pages+1,1):
                currentPageLog, total_pages, total_records, current_page = logPageDownloader(_TimeFrom, _TimeTo,
                                                                                             _vehicleID, pageNumber, rows,
                                                                                             useSaved=True,
                                                                                             vehicleName=_vehicleName)
                list2return.extend( currentPageLog )

        # saving scraped request:
        with open( cashed_file, 'wb' ) as f:
            pickle.dump( list2return, f )

        return list2return


def type2SQL(tipe):
    match tipe.__name__:
        case 'int':
            return 'INTEGER'
        case 'float':
            return 'REAL'
        case 'str':
            return 'TEXT'
        case 'bool':
            return 'INTEGER'
        case 'list':
            return 'TEXT'
        case 'NoneType':
            return 'NULL'


def oneTime_FiledsDefinitionGetter(log):
    # getting types of values for SQL
    tipez = {kee: type(val) for kee, val in log[0].items()}
    # converting types 2 SQLite types
    dd = {kee: type2SQL(val) for kee, val in tipez.items()}
    fields2create = "".join([f'{kee} {val},' for kee,val in dd.items()])[:-1]
    return fields2create


def oneTime_logExampleGetter():
    print('log getting')
    #get some journals for testing
    _vehicleID = 1219000566
    _TimeFrom = Date2Onix( '2022', '11', '15', '14' )
    _TimeTo = Date2Onix( '2022', '11', '15', '15', )
    log = get_LOG(_TimeFrom, _TimeTo, _vehicleID)
    return log


def create_journal_table(cursor):
    print('cretion of journal')
    # preparation for table creation:
    log = oneTime_logExampleGetter()
    fields2create = oneTime_FiledsDefinitionGetter(log)
    # correcting fields
    fields2create = fields2create.replace("id INTEGER", "carID INTEGER")
    # create a table
    cursor.execute(f'PRAGMA foreign_keys=ON;')
    # creation based on reallog example:
    # cursor.execute(f'CREATE TABLE journal ({fields2create}, FOREIGN KEY("carID") REFERENCES cars("omniIDxl"));')
    cursor.execute(f'''
        CREATE TABLE "journal" (
                                "carID" INTEGER,
                                "tImp" INTEGER,
                                "alarm" INTEGER,
                                "amtrX" REAL,
                                "amtrY" REAL,
                                "amtrZ" REAL,
                                "image" INTEGER,
                                "charge" REAL,
                                "univ1" REAL,
                                "univ2" REAL,
                                "SPN110" INTEGER,
                                "SPN190" REAL,
                                "SPN245" REAL,
                                "SPN250" INTEGER,
                                "errors" TEXT,
                                "isOpen" INTEGER,
                                "supply" REAL,
                                "isGSMOn" INTEGER,
                                "mileage" REAL,
                                "TIME_DVR" TEXT,
                                "altitude" REAL,
                                "delivery" INTEGER,
                                "iButton2" TEXT,
                                "latitude" REAL,
                                "lls1Code" INTEGER,
                                "lls2Code" INTEGER,
                                "lls3Code" INTEGER,
                                "lls4Code" INTEGER,
                                "lls5Code" INTEGER,
                                "lls6Code" INTEGER,
                                "speedGPS" REAL,
                                "speedImp" INTEGER,
                                "DVR_ERROR" INTEGER,
                                "direction" INTEGER,
                                "eventDate" INTEGER,
                                "eventMask" INTEGER,
                                "lls1Exist" INTEGER,
                                "lls1Ready" INTEGER,
                                "lls2Exist" INTEGER,
                                "lls2Ready" INTEGER,
                                "lls3Exist" INTEGER,
                                "lls3Ready" INTEGER,
                                "lls4Exist" INTEGER,
                                "lls4Ready" INTEGER,
                                "lls5Exist" INTEGER,
                                "lls5Ready" INTEGER,
                                "lls6Exist" INTEGER,
                                "lls6Ready" INTEGER,
                                "longitude" REAL,
                                "uniStates" INTEGER,
                                "driverCode" TEXT,
                                "gpsJamming" INTEGER,
                                "gsmJamming" INTEGER,
                                "SERVER_CONN" INTEGER,
                                "accelStatus" INTEGER,
                                "coolantTemp" INTEGER,
                                "discreteOut" INTEGER,
                                "isConnected" INTEGER,
                                "isRoamingOn" INTEGER,
                                "CAM_RECORD_1" INTEGER,
                                "CAM_RECORD_2" INTEGER,
                                "CAM_RECORD_3" INTEGER,
                                "CAM_RECORD_4" INTEGER,
                                "acceleration" REAL,
                                "existCANdata" INTEGER,
                                "existGPSdata" INTEGER,
                                "existLLSdata" INTEGER,
                                "isIgnitionOn" INTEGER,
                                "supplyStatus" INTEGER,
                                "SDCARD_FREE_1" TEXT,
                                "SDCARD_FREE_2" TEXT,
                                "SIGNAL_LOST_1" INTEGER,
                                "SIGNAL_LOST_2" INTEGER,
                                "SIGNAL_LOST_3" INTEGER,
                                "SIGNAL_LOST_4" INTEGER,
                                "authorization" INTEGER,
                                "satellitesNmb" INTEGER,
                                "isGPSDataValid" INTEGER,
                                "SDCARD_STATUS_1" INTEGER,
                                "SDCARD_STATUS_2" INTEGER,
                                "lls1Temperature" INTEGER,
                                "lls2Temperature" INTEGER,
                                "lls3Temperature" INTEGER,
                                "lls4Temperature" INTEGER,
                                "lls5Temperature" INTEGER,
                                "lls6Temperature" INTEGER,
                                "authorizationEnd" INTEGER,
                                "lls1ErrorOccured" INTEGER,
                                "lls2ErrorOccured" INTEGER,
                                "lls3ErrorOccured" INTEGER,
                                "lls4ErrorOccured" INTEGER,
                                "lls5ErrorOccured" INTEGER,
                                "lls6ErrorOccured" INTEGER,
                                "SDCARD_CAPACITY_1" TEXT,
                                "SDCARD_CAPACITY_2" TEXT,
                                "safeDrivingSource" TEXT,
                                "isCalibTableExist1" INTEGER,
                                "isCalibTableExist2" INTEGER,
                                "isCalibTableExist3" INTEGER,
                                "isCalibTableExist4" INTEGER,
                                "isCalibTableExist5" INTEGER,
                                "isCalibTableExist6" INTEGER,
                                FOREIGN KEY("carID") REFERENCES "cars"("omniIDxl")
                                 )
                ''')



def check_if_journal_table_exist(cursor):
    # get the count of tables with the name
    cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='journal' ''')
    # if the count is 1, then table exists
    if cursor.fetchone()[0] == 1:
        print('table journal EXIST')
        return True
    else:
        print('table journal DOES NOT EXIST')
        return False


def check_if_cars_table_exist(cursor):
    # get the count of tables with the name
    cursor.execute(''' SELECT count(name) FROM sqlite_master WHERE type='table' AND name='cars' ''')
    # if the count is 1, then table exists
    if cursor.fetchone()[0] == 1:
        print('table car EXIST')
        return True
    else:
        print('table car DOES NOT EXIST')
        return False


def create_cars_table(connection, cars_xl_list_path):
    print('creation of cars table')
    # preparation for table creation:
    carsDf = pd.read_excel(cars_xl_list_path)
    carsDf.to_sql(name='cars',con=connection, if_exists='fail', index=False,)


def alterCarsTableToAddPrimaryKey(cursor):
    cursor.execute("""
    PRAGMA foreign_keys=off;
    BEGIN TRANSACTION;
    ALTER TABLE cars RENAME TO old_cars;
    CREATE TABLE cars
    (
      employee_id INTEGER,
      last_name VARCHAR NOT NULL,
      first_name VARCHAR,
      hire_date DATE,
      CONSTRAINT employees_pk PRIMARY KEY (employee_id)
    );
    
    INSERT INTO employees SELECT * FROM old_employees;
    
    COMMIT;
    
    PRAGMA foreign_keys=on;
    """)

def alterJournalTableToAddForeignKey(cursor):
    cursor.execute("""
    PRAGMA foreign_keys=ON;
    BEGIN TRANSACTION;
    ALTER TABLE cars RENAME TO old_cars;
    CREATE TABLE cars
    (
      employee_id INTEGER,
      last_name VARCHAR NOT NULL,
      first_name VARCHAR,
      hire_date DATE,
      CONSTRAINT employees_pk PRIMARY KEY (employee_id)
    );

    INSERT INTO employees SELECT * FROM old_employees;

    COMMIT;

    PRAGMA foreign_keys=on;
    """)



def convert2SQLiteFormat(val):
    match val:
        case True:
            return 1
        case False:
            return 0
        case None:
            return ''
        case _:
            return val


def matchLogLineWithTableFields(line):
    emptyLine = {'carID': 0,
                'tImp': 0,
                'alarm': 0,
                'amtrX': 0.0,
                'amtrY': 0.0,
                'amtrZ': 0.0,
                'image': 0,
                'charge': 0.0,
                'univ1': 0.0,
                'univ2': 0.0,
                'SPN110': 0,
                'SPN190': 0.0,
                'SPN245': 0.0,
                'SPN250': 0,
                'errors': '',
                'isOpen': 0,
                'supply': 0.0,
                'isGSMOn': 0,
                'mileage': 0.0,
                'TIME_DVR': '',
                'altitude': 0.0,
                'delivery': 0,
                'iButton2': '',
                'latitude': 0.0,
                'lls1Code': 0,
                'lls2Code': 0,
                'lls3Code': 0,
                'lls4Code': 0,
                'lls5Code': 0,
                'lls6Code': 0,
                'speedGPS': 0.0,
                'speedImp': 0,
                'DVR_ERROR': 0,
                'direction': 0,
                'eventDate': 0,
                'eventMask': 0,
                'lls1Exist': 0,
                'lls1Ready': 0,
                'lls2Exist': 0,
                'lls2Ready': 0,
                'lls3Exist': 0,
                'lls3Ready': 0,
                'lls4Exist': 0,
                'lls4Ready': 0,
                'lls5Exist': 0,
                'lls5Ready': 0,
                'lls6Exist': 0,
                'lls6Ready': 0,
                'longitude': 0.0,
                'uniStates': 0,
                'driverCode': '',
                'gpsJamming': 0,
                'gsmJamming': 0,
                'SERVER_CONN': 0,
                'accelStatus': 0,
                'coolantTemp': 0,
                'discreteOut': 0,
                'isConnected': 0,
                'isRoamingOn': 0,
                'CAM_RECORD_1': 0,
                'CAM_RECORD_2': 0,
                'CAM_RECORD_3': 0,
                'CAM_RECORD_4': 0,
                'acceleration': 0.0,
                'existCANdata': 0,
                'existGPSdata': 0,
                'existLLSdata': 0,
                'isIgnitionOn': 0,
                'supplyStatus': 0,
                'SDCARD_FREE_1': '',
                'SDCARD_FREE_2': '',
                'SIGNAL_LOST_1': 0,
                'SIGNAL_LOST_2': 0,
                'SIGNAL_LOST_3': 0,
                'SIGNAL_LOST_4': 0,
                'authorization': 0,
                'satellitesNmb': 0,
                'isGPSDataValid': 0,
                'SDCARD_STATUS_1': 0,
                'SDCARD_STATUS_2': 0,
                'lls1Temperature': 0,
                'lls2Temperature': 0,
                'lls3Temperature': 0,
                'lls4Temperature': 0,
                'lls5Temperature': 0,
                'lls6Temperature': 0,
                'authorizationEnd': 0,
                'lls1ErrorOccured': 0,
                'lls2ErrorOccured': 0,
                'lls3ErrorOccured': 0,
                'lls4ErrorOccured': 0,
                'lls5ErrorOccured': 0,
                'lls6ErrorOccured': 0,
                'SDCARD_CAPACITY_1': '',
                'SDCARD_CAPACITY_2': '',
                'safeDrivingSource': '',
                'isCalibTableExist1': 0,
                'isCalibTableExist2': 0,
                'isCalibTableExist3': 0,
                'isCalibTableExist4': 0,
                'isCalibTableExist5': 0,
                'isCalibTableExist6': 0}
    emptyLine.update(line)
    return emptyLine


def convUnistates(states):
    return int(''.join(states.replace('False','0').replace('True','1')).lstrip('[').rstrip(']').replace(',','')[::2])


def logInserter(log,connection,_vehicleID):
    # inserting new info into db:
    for line in log:
        convertedDiq = {key: str(val) for key, val in line.items()}
        convertedDiq.pop('id')
        convertedDiq.update({'carID': _vehicleID})
        if 'uniStates' in convertedDiq:
            convertedDiq['uniStates'] = convUnistates(convertedDiq['uniStates'])
        matchedDiq = matchLogLineWithTableFields(convertedDiq)
        try:
            connection.execute(f"INSERT INTO journal VALUES {tuple(matchedDiq.values())}")
        except:
            pass
            # print(Exception)
    connection.commit()


def logRetrive(car,dateFrom,dateTo,connection):
    _vehicleID = car[0]
    carName = OMNI2CarName(_vehicleID)
    print(f'downloading logs for {carName}')
    for dayNumber in range((dateTo - dateFrom).days):
        day = dateFrom + td(dayNumber)
        _TimeFrom = dateTime2Onix(day)
        _TimeTo = dateTime2Onix(day + td(1))
        print(f"getting {day.date()} for {car}{carName}")
        log = get_LOG(_TimeFrom, _TimeTo, _vehicleID, useSaved=True,_vehicleName=carName)
        logInserter(log, connection, _vehicleID)

        print(f'>> Saved:  {carName} on {day}')

#%%
# ioloop = asyncio.get_event_loop()
def main():
    # Datatypes:
    # NULL
    # INTEGER
    # REAL
    # TEXT
    # BLOB
    cars_xl_list_path = '/home/user/PYTHON/Projects/DSM/venv/_lists/listAUTO_fullList.xlsx'
    path2DB = r'/home/user/PYTHON/Projects/DB/'
    DBname = 'journal.sqlite'
    # connect to database
    connection = sqlite3.connect(path2DB+DBname)
    # create a cursor
    cursor = connection.cursor()

    # check if cars table exist
    if check_if_cars_table_exist(cursor) == False:
        create_cars_table(connection, cars_xl_list_path)
        connection.commit()

    # check if journal table exist
    if check_if_journal_table_exist(cursor) == False:
        create_journal_table(cursor)
        connection.commit()

    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS ndx_car_time ON journal(carID, eventDate);")
    cursor.execute("CREATE INDEX IF NOT EXISTS ndx_car ON journal(carID);")
    cursor.execute("CREATE INDEX IF NOT EXISTS ndx_time ON journal(eventDate);")
    connection.commit()
    # # getting info for insertion:
    # _vehicleID = 1219001271

    dateFrom  = dt(2022,11,1,0,0,0)
    dateTo = dt(2022,11,17,21,0,0)

    cursor.execute(f"SELECT omniIDxl from cars")
    # cars = cursor.fetchall()[44:45]
    cars = cursor.fetchall()[86:]
    totalCars = len(cars)
    print(f'Total car {totalCars}')

    global ioloop

    for num in range(1,totalCars):
        # time.sleep(0.33)
        logRetrive(cars.pop(0), dateFrom, dateTo, connection)


    # for car in cars:
    #     tasks = [ioloop.create_task(logRetrive(cars.pop(0),dateFrom,dateTo,connection)),
    #              ioloop.create_task(logRetrive(cars.pop(0),dateFrom,dateTo,connection)),
    #              ioloop.create_task(logRetrive(cars.pop(0),dateFrom,dateTo,connection))]
        # logRetrive(car,dateFrom,dateTo,connection)

    #     wait_tasks = asyncio.wait(tasks)
    #     ioloop.run_until_complete(wait_tasks)
    # ioloop.close()


    # # query the DB:
    # cursor.execute("SELECT rowid, * FROM journal ORDER BY rowid DESC  LIMIT 10")
    # items = cursor.fetchall()

    # commit
    connection.commit()
    # Close connection
    connection.close()


if __name__=='__main__':
    main()