import requests
import json
import time
import re
import os
from datetime import datetime as dt
from datetime import timedelta as td
# from log2file import *


def getJWTFile():
    empty = {'jwt': '', 'refresh': '', 'exp': 0}

    try:
        file = open( r'authJWT.txt', 'r', encoding='utf-8' )
        Jstring = (file.read())
        file.close()
    except:
        return empty

    jwtFile = json.loads(re.sub("\'", '\"', Jstring))
    JWT = jwtFile['jwt']
    REFRESH = jwtFile['refresh']
    EXP = int(jwtFile['exp'])

    try:
        jdic = {'jwt':JWT,'refresh':REFRESH, 'exp':EXP}
    except:
        jdic = empty
    return jdic


def login():
    print('gettn new auth token..')
    # log=start_logger('auth.log')
    #
    # def print(msg):
    #     logg = log
    #     logg.info(msg)

    pwdfile = open(r"/home/user/PYTHON/API/DSM/api-auth.pwd",'r')
    login,password = pwdfile.readline().split(' ')
    password = password.strip('\n')

    timeMult = 1.3333
    time2wait = 0.1
    response = False
    trys = 10
    while response == False:
        try:
            response = requests.post(
                'https://online.omnicomm.ru/auth/login?jwt=1',
                data={"login": login, "password": password})
            if not response:
                time2wait *= timeMult
                # print(time2wait)
                print(f'>>>>..empty response........ waiting for authorization {time2wait}........')
                time.sleep(time2wait)
                continue
        except:
            time2wait *= timeMult
            print(f'>>>>..connection error.... waiting for authorization {time2wait}........')
            time.sleep(time2wait)

    JWT = response.json().get('jwt')
    REFRESH = response.json().get('refresh')
    EXP = int(dt.now().timestamp())
    freshJWT = {'jwt': JWT, 'refresh': REFRESH, 'exp': EXP}
    # saving 2 file:
    with open('authJWT.txt', 'w', encoding='utf-8') as fjwt:
        fjwt.write(json.dumps(freshJWT))
    return freshJWT



def refresh(jwt):
    print('refreshing auth token...')
    freshjwt=None
    response = False
    timeMult = 1.3333
    time2wait = 0.1
    if not jwt['refresh']:
        return auth()
    while response == False:
        try:
            Headers = {'accept': 'application/json',
                       'Authorization':  'JWT ' + jwt['refresh'],
                       'Content-Type': 'application/json'}
            response = requests.post(
                'https://online.omnicomm.ru/auth/refresh', headers=Headers)
            if not response:
                print(f'>>>>.......... waiting for REauthorization {time2wait}........')
                time2wait *= timeMult
                time.sleep(time2wait)
                return login()
            elif response.status_code==401:
                print('not authorized')
                return login()
        except:
            print(f'>>>>...connection error.... waiting for authorization {time2wait}........')
            time2wait *= timeMult
            time.sleep(time2wait)

    if not freshjwt:
        JWT = response.json().get('jwt')
        REFRESH = response.json().get('refresh')
    # else:
    #     JWT = freshjwt['jwt']
    #     REFRESH = freshjwt['refresh']
    EXP = dt.now().timestamp()
    freshjwt = {'jwt': JWT, 'refresh': REFRESH, 'exp': int(EXP)}
    # saving 2 file:
    with open('authJWT.txt', 'w', encoding='utf-8') as fjwt:
        fjwt.write(json.dumps(freshjwt))
    return freshjwt



def auth():

    jwt = getJWTFile()
    if jwt['jwt'] == '' or jwt['jwt'] == None:
        jwt = login()
    else:
        if dt.now().timestamp()-jwt['exp'] >= 59:
            # print(f'dt.now().timestamp()={dt.now().timestamp()}-jwt["exp"]={jwt["exp"]} == {dt.now().timestamp()-jwt["exp"]}')
            jwt = refresh(jwt)
    return jwt




    # pwdfile = open(r"/home/user/PYTHON/API/DSM/api-auth.pwd",'r')
    # login,password = pwdfile.readline().split(' ')
    # password = password.strip('\n')
    # auth_string = JWT_FILE.getJWTfromFile()
    # Headers = {'JWT ':auth_string['jwt']}
    # response = False
    # time2wait = 0.1
    # while response == False:
    #     try:
    #         response = requests.post( 'https://online.omnicomm.ru/auth/refresh/', headers=Headers )
    #     except:
    #         time2wait *=1.68
    #         time.sleep(time2wait)
    #         # print(f'>>>>>>>>> AUTH connection ERROR !!!!')
    #         print(f'>>> connection ERROR ! waiting authorizing {time2wait}')
    #
    # REFRESH = auth_string['refresh']
    # JWT = auth_string['jwt']
    # if json.loads( response.text )['error'] == 'Unauthorized':
    #     timeMult = 1.3333
    #     time2wait = 0.1
    #     response = False
    #     while response == False:
    #         try:
    #             response = requests.post(
    #                 'https://online.omnicomm.ru/auth/login?jwt=1',
    #                 data={"login": login, "password": password})
    #             if not response:
    #                 time2wait *= timeMult
    #                 print(time2wait)
    #                 time.sleep(time2wait)
    #                 print(f'>>>>..empty response........ waiting for authorization {time2wait}........')
    #                 continue
    #         except:
    #             time2wait *= timeMult
    #             time.sleep(time2wait)
    #             print(f'>>>>..connection error.... waiting for authorization {time2wait}........')
    #
    #     JWT = response.json().get( 'jwt' )
    #     REFRESH = response.json().get( 'refresh' )
    #
    #     fjwt = open( 'authJWT.txt', 'w', encoding='utf-8' )
    #     fjwt.write( str( response.json() ) )
    #     fjwt.close()
    # jdic = {}
    # jdic = {'jwt': JWT, 'refresh': REFRESH}
    # return jdic



def main():
    t1 = dt.now()
    jwt = auth()
    print(dt.now() - t1)
    # formerJWTexp  = 0
    # print('authorize test')
    # for i in range(10):
    #     startTime = dt.now()
    #     JWT = auth()
    #     JWTexp = int(JWT['exp'])
    #     endTime = dt.now()
    #
    #     print(f'{JWT}')
    #     if JWTexp != formerJWTexp:
    #         print('got new jwt')
    #         formerJWTexp = JWTexp
    #
    #     duration = endTime - startTime
    #     print(f'took {duration}')
    #     time.sleep(30)


if __name__=='__main__':
    main()