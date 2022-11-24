#%% imports
import sys
sys.path.append('/home/user/PYTHON/Projects/DSM/venv/')
sys.path.append('/home/user/PYTHON/Projects/DB/venv/')
import sqlite3
import os
import pickle
import re
import requests
import json
from TIME_OMNI import *
from authorize_async import *
import pandas as pd
import openpyxl
import warnings
warnings.simplefilter("ignore")
import asyncio
import aiohttp
import time
import subprocess



def GetPath2DBfile():
    myip=subprocess.run("wget -O - -q icanhazip.com".split(' '), stdout=subprocess.PIPE, text=True).stdout[:-1]
    if myip == '195.239.228.234':
        return '/mnt/DB/journal.sqlite'
    else:
        return '/home/user/PYTHON/Projects/DB/journal.sqlite'


async def autorizing():
    jwt = await auth()['jwt']
    # await asyncio.sleep(62)


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


def CarName(id,cursor):
    cursor.execute(f"SELECT name from cars WHERE omniIDxl={id}")
    return cursor.fetchall()[0][0]


#https://stackoverflow.com/questions/35196974/aiohttp-set-maximum-number-of-requests-per-second#:~:text=0%2C%20when%20using%20a%20ClientSession,of%20simultaneous%20connections%20to%20100.&text=In%20case%20it's%20better%20suited,to%20the%20same%20%22endpoint%22.
class Limiter:
    def __init__(self, calls_limit: int = 5, period: int = 1):
        self.calls_limit = calls_limit
        self.period = period
        self.semaphore = asyncio.Semaphore(calls_limit)
        self.requests_finish_time = []

    async def sleep(self):
        if len(self.requests_finish_time) >= self.calls_limit:
            sleep_before = self.requests_finish_time.pop(0)
            if sleep_before >= time.monotonic():
                await asyncio.sleep(sleep_before - time.monotonic())

    def __call__(self, func):
        async def wrapper(*args, **kwargs):

            async with self.semaphore:
                await self.sleep()
                res = await func(*args, **kwargs)
                self.requests_finish_time.append(time.monotonic() + self.period)

            return res

        return wrapper



@Limiter(calls_limit=7, period=1)
async def get_LOG_page(_TimeFrom, _TimeTo, _vehicleID, session, page=1, rows=250, vehicleName='', action="getReportData", useSaved=True ):   # Onix time !
    # print('async def get_LOG_page')
    JWT =  auth()['jwt']
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

    st=0
    while st !=200:
        try:

            waitTime=0.33
            await asyncio.sleep(waitTime)

            print(f'-------->>  start session.post page№ {page} rows={rows} {vehicleName}')
            print('_______________________________________________________________________')
            async with session.post( 'https://online.omnicomm.ru/service/reports/', data=Params, headers=Headers ) as server_answer:

                # waitTime = 0.33
                # await asyncio.sleep(waitTime)
                DATA = await server_answer.json(loads=json.loads)
                time_taken_for_request = dt.strptime(server_answer.raw_headers[1][1].decode(),'%a, %d %b %Y %H:%M:%S %Z')-dt.utcnow()
                # print('GOT log page №',page,'(rows =',rows,") ",vehicleName,server_answer.status,"->",time_taken_for_request)
                st = server_answer.status
        except Exception as e:
            print(e)
            waitTime=0.33
            await asyncio.sleep(waitTime)
            print(f'retrying to load agian PAGE#_{page}  {_vehicleID} {vehicleName} ')
            pass
        else:
            try:

                total_pages =  DATA['results']['total']
                total_records =  DATA['results']['records']
                current_page =  DATA['results']['page']
                pagelog =  DATA['results']['rows']
            except Exception as e:
                print(e)
            waitTime=0.33
            await asyncio.sleep(waitTime)

    return pagelog,total_pages,total_records,current_page


async def logPageDownloader(_TimeFrom, _TimeTo, _vehicleID, session, pageN, rows, useSaved=True, vehicleName='car'):
    total_pages = 0
    total_records = 0
    current_page = 0
    success = False

    t0=time.time()
    # print('__>> logPageDownloader')
    currentPageLog, total_pages, total_records, current_page = await get_LOG_page(_TimeFrom, _TimeTo, _vehicleID,
                                                                                  session, pageN,
                                                                                  rows, useSaved=useSaved,
                                                                                  vehicleName=vehicleName)
    print(f'<<---- loaded page# <{pageN}> per {rows} out of <<{int(total_records/rows)} | {total_records}>> on car={vehicleName}  >>>> {Onix2Date(_TimeFrom)} >>>>> {Onix2Date(_TimeTo)}')


    #
    # while success == False:
    #     try:
    #         print(f'loading page# {pageN} on car={vehicleName} from=> {Onix2Date(_TimeFrom)} to=> {Onix2Date(_TimeTo)}')
    #         currentPageLog, total_pages, total_records, current_page = await get_LOG_page(_TimeFrom, _TimeTo, _vehicleID,session, pageN,
    #                                                                                 rows, useSaved=useSaved,
    #                                                                                 vehicleName=vehicleName)
    #     except:
    #         success = False
    #         print(f'>> logPageDownloader ERR car={vehicleName} page# {pageN}')
    #         await asyncio.sleep(1)
    #     else:
    #         success = True
    return currentPageLog, total_pages, total_records, current_page



async def get_LOG(_TimeFrom, _TimeTo, _vehicleID, session, rows=250, action="getReportData", useSaved=True, save2file=False ,_vehicleName=''):
    # print('__>> get_LOG')
    dateDate = Onix2Date(_TimeFrom).date()
    projectDir=r'/home/user/PYTHON/Projects/DSM/venv'
    dirName = f"{projectDir}/Cashed_requests/_LOGS/{dateDate}"
    if save2file and not os.path.exists(dirName):
        os.mkdir(dirName)
    cashed_file = f"{projectDir}/Cashed_requests/_LOGS/{dateDate}/log-{_TimeFrom}_{_TimeTo}_{_vehicleID}.pydata"
    if useSaved and os.path.exists(cashed_file):
        with open( cashed_file, 'rb' ) as pickleRick:
            log = pickle.load( pickleRick )
            return log
    else:
        # print( f"LOG:{Onix2Date( _TimeFrom )}->{Onix2Date( _TimeTo )} {_vehicleName}" )
        list2return = []
        total_pages = 0
        total_records = 0
        current_page = 0
        # t0=time.time()

        currentPageLog, total_pages, total_records, current_page = await logPageDownloader(_TimeFrom, _TimeTo, _vehicleID, session, 1, rows, useSaved=useSaved, vehicleName=_vehicleName)
        list2return.extend(currentPageLog)

        print(f"<<__ got_1_LOG {_vehicleID} {_vehicleName} {Onix2Date(_TimeFrom)} to {Onix2Date(_TimeTo)} Total pages = {total_pages} Total records = {total_records}")

        if total_pages > 1:
            tasks = []
            loop = asyncio.get_event_loop()
            # print('loop.create_task(list2return.extend(logPageDownloader(_TimeFrom, _TimeTo,')
            async def nextPageGetter(_TimeFrom, _TimeTo, _vehicleID, session,
                                                                  pageNumber, rows,list2return, useSaved=True,
                                                                 vehicleName=_vehicleName):
                currentPageLog, _, _, _ = await logPageDownloader(_TimeFrom, _TimeTo, _vehicleID, session,
                                                                  pageNumber, rows, useSaved=True,
                                                                 vehicleName=_vehicleName)
                list2return.extend( currentPageLog )

            for pageNumber in range(2,total_pages+2,1):
                task = asyncio.create_task(nextPageGetter(_TimeFrom, _TimeTo, _vehicleID, session,
                                                                  pageNumber, rows,list2return, useSaved=True,
                                                                 vehicleName=_vehicleName))

                tasks.append(task)
            await asyncio.gather(*tasks)

        # saving scraped request:
        if save2file:
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


async def logRetrive(car,dateFrom,dateTo,connection,session):
    _vehicleID = car[0]
    carName = CarName(_vehicleID,connection.cursor())
    # print(f'downloading logs for {carName}')

    log = await get_LOG(dateTime2Onix(dateFrom), dateTime2Onix(dateTo), _vehicleID, session, useSaved=False,_vehicleName=carName)
    logInserter(log, connection, _vehicleID)
    print(f'SAVED logs for {carName}')

#%%
# ioloop = asyncio.get_event_loop()
async def main():

    started = dt.utcnow()
    auth()
    # Datatypes:
    # NULL
    # INTEGER
    # REAL
    # TEXT
    # BLOB
    # cars_xl_list_path = '/home/user/PYTHON/Projects/DSM/venv/_lists/listAUTO_fullList.xlsx'
    # path2DB = r'/home/user/PYTHON/Projects/DB/'
    # DBname = 'journal.sqlite'
    path2DBfile = GetPath2DBfile()
    # connect to database
    connection = sqlite3.connect(path2DBfile)
    # create a cursor
    cursor = connection.cursor()
    cursor.execute(f"SELECT Count(*) FROM journal")
    totalRecordsWAS = cursor.fetchall()[0][0]
    print(f'Total records = {totalRecordsWAS}')
    # # check if cars table exist
    # if check_if_cars_table_exist(cursor) == False:
    #     create_cars_table(connection, cars_xl_list_path)
    #     connection.commit()
    #
    # # check if journal table exist
    # if check_if_journal_table_exist(cursor) == False:
    #     create_journal_table(cursor)
    #     connection.commit()

    cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS ndx_car_time ON journal(carID, eventDate);")
    cursor.execute("CREATE INDEX IF NOT EXISTS ndx_car ON journal(carID);")
    cursor.execute("CREATE INDEX IF NOT EXISTS ndx_time ON journal(eventDate);")
    connection.commit()
    # # getting info for insertion:
    # _vehicleID = 1219001271

    dateFrom  = dt(2022,10,2,0,0,0)
    dateTo =    dt(2022,10,31,0,0,0)

    cursor.execute(f"SELECT omniIDxl from cars")
    # cursor.execute(f"SELECT omniIDxl from cars where omniIDxl='1219000601'")

    # cars = cursor.fetchall()[44:45]
    cars = cursor.fetchall()
    totalCars = len(cars)
    print(f'Total car {totalCars}')

    tasks = []
    # tasks.append(asyncio.create_task(autorizing()))
    async with aiohttp.ClientSession() as session:
        for num in range(0,totalCars):
            # time.sleep(0.33)
            task = asyncio.create_task(logRetrive(cars.pop(0), dateFrom, dateTo, connection,session))
            tasks.append(task)

        await asyncio.gather(*tasks)


    # # query the DB:
    # cursor.execute("SELECT rowid, * FROM journal ORDER BY rowid DESC  LIMIT 10")
    # items = cursor.fetchall()


    print('done!')
    cursor.execute(f"SELECT Count(*) FROM journal")
    totalRecordsBECAME = cursor.fetchall()[0][0]
    print(f'taken {dt.utcnow()-started} written total: {totalRecordsBECAME - totalRecordsWAS} records')
    # commit
    connection.commit()
    # Close connection
    connection.close()

if __name__=='__main__':
    # main()
    asyncio.run(main())