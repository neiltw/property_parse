# -*- coding: UTF-8 -*-


import pandas as pd
import json
import zipfile
# from pandas.io.json import json_normalize
import unicodedata
# from decimal import Decimal
import numpy as np
import msgpack
import ast
import sqlite3

# input category type result int
def getCategory(title,data,number):
    # print '------ ',data
    result = 0
    equalTitle = False
    category_map = 'category_map.txt'
    getCategory = open(category_map , mode='r' , )
    strAdd = ""
    for c in getCategory:
        filterTab = c.replace("\r", '').replace("\n", '').replace("\t","").replace(" ","")
        strAdd = strAdd + filterTab
    dictData = strAdd.split("=")[1]
    titleData = ast.literal_eval(dictData)
    unicodeTransString = unicodedata.normalize('NFKD', title[number].decode('big5')).encode('utf8') # unicode to str

    for getMainTtiel in titleData:
        if unicodeTransString == getMainTtiel:
            # print title[number].decode('big5'), getMainTtiel
            for getSubData in titleData[getMainTtiel]:
                if getSubData == data :
                    result = titleData[getMainTtiel][getSubData]
                    equalTitle = True
                    # print data,  titleData[getMainTtiel][getSubData], getSubData
                    return result
            if equalTitle == True: break

    return result

def Show(conn,tableName):
    select = 'SELECT * FROM %s limit 10' % (tableName)
    for row in conn.execute(select):
        print row
    conn.commit()

def TransferDecodeEncode(st):
    return st)


def crSQL(sql, number,tableName):
    sql = sql % (tableName)
    for row in range(len(number[0])):
        if row == len(number[0]) - 1:
            sql = sql + '?)'
        else:
            sql = sql + '?,'
    # print 'insert= ', sql
    return sql


def TEP_Park(infoFile,nu):
    resultList = []
    titleName = []
    for line in infoFile:
        # if nu == 100: break
        if nu == 0:
            title = line.split(',')
            for getTitle in title:  # get title name
                titleName.append(getTitle)
        if nu != 0:
            try:
                subData = line.split(',')
                serial = TransferDecodeEncode(subData[0])
                categoryTfInt = TransferDecodeEncode(subData[1])
                category = int( getCategory(titleName, categoryTfInt,1 ) )
                if TransferDecodeEncode(subData[2]) == '':
                    price = None
                else:
                    price = int(TransferDecodeEncode(subData[2]))
                if TransferDecodeEncode(subData[3]) == '':  # check area is null
                    area = None
                else:
                    area = float(TransferDecodeEncode(subData[3]))
                result = (serial, category, price, area)
                resultList.append(result)
                # print result
            except:
                print '-------english name not using and Special symbol -------' , subData[0] +',',  subData[1] +',',  subData[2] +',',  subData[3]
        nu = nu + 1

    print 'count:', len(resultList)
    print 'row:', len(resultList[0])

    tableName = 'park'
    sql = 'insert into %s (serial_number,category,price,area)values ('
    sqlinsert = crSQL(sql,resultList,tableName)
    conn.cursor()
    conn.text_factory = str # insert chinese
    conn.executemany(sqlinsert, resultList)
    Show(conn,tableName)

def TPE_Land(infoFile, nu):
    result = []
    titleName = []
    for line in infoFile:
        # if nu == 100 : break
        if nu == 0:
            title = line.split(',')
            for getTitle in title:  # get title name
                if TransferDecodeEncode(getTitle) == '使用分區或編定':
                    stt  = unicodedata.normalize('NFKD', u'都市土地使用分區').encode('big5') # unicode transform to str
                    titleName.append(stt)
                    # print 'change name'  , getTitle.decode('big5')
                else:
                    titleName.append(getTitle)
        if nu != 0:
            try:
                subData = line.split(',')
                serial = TransferDecodeEncode(subData[0])
                # try:
                position = TransferDecodeEncode(subData[1])
                # except:
                    # position = subData[1].replace("\r", '').replace("\n",'')
                # print subData[2].decode('big5').encode('utf8').replace("\r", '').replace("\n",'')
                if TransferDecodeEncode(subData[2]) == '':  # check area is null
                    area = None
                else:
                    area = float(TransferDecodeEncode(subData[2]))
                zoningSplit = TransferDecodeEncode(subData[3]).split('：')[1]
                zoning = int(getCategory(titleName, zoningSplit,3))
                list = (serial, position, area, zoning)
                # print [number, position, area, zoning]
                result.append(list)
            except:
                print '-------english name not using and Special symbol-------' , subData[0] +',',  subData[1] +',',  subData[2] +',',  subData[3]
        nu = nu +1

    print 'count:', len(result)
    print 'row:', len(result[0])

    tableName = 'land'
    sql = 'insert into %s (serial_number ,sector_position,shifting_area,zoning)values ('
    sqlinsert = crSQL(sql,result,tableName)
    conn.cursor()
    conn.text_factory = str # insert chinese
    conn.executemany(sqlinsert, result)
    Show(conn,tableName)

def TPE_Build(infoFile,nu):
    result = []
    titleName = []
    for line in infoFile:
        # if nu == 100 : break
        if nu == 0:
            title = line.split(',')
            for getTitle in title:  # get title name
                titleName.append(getTitle)
        if nu != 0:
            try:
                subData = line.split(',')
                serial = TransferDecodeEncode(subData[0])
                if TransferDecodeEncode(subData[1]) == '':
                    room_age = None
                else:
                    room_age = int(TransferDecodeEncode(subData[1]))
                if TransferDecodeEncode(subData[2]) == '':
                    buildArea = None
                else:
                    buildArea = float(TransferDecodeEncode(subData[2]))

                mainUseTfInt = TransferDecodeEncode(subData[3])
                mainUse = int(getCategory(titleName,mainUseTfInt,3))

                mainMaterialsTfInt = TransferDecodeEncode(subData[4])
                mainMaterials = int(getCategory(titleName,mainMaterialsTfInt,4))

                completesDate = TransferDecodeEncode(subData[5])
                totalLayer = TransferDecodeEncode(subData[6])
                lamination = TransferDecodeEncode(subData[7])
                # print serial , room_age, buildArea ,mainUse, mainMaterials,completesDate,totalLayer,lamination
                list = (serial , room_age, buildArea ,mainUse, mainMaterials,completesDate,totalLayer,lamination)
                result.append(list)
            except:
                print '-------english name not using and Special symbol -------' , subData[0] +',',  subData[1] +',',  subData[2] +',',  subData[3]
        nu = nu + 1

    print 'count:', len(result)
    print 'row:', len(result[0])

    tableName = 'build'
    sql = 'insert into %s (serial_number  , room_age  ,building_shifting  ,main_us  ,main_building_materials ,construction_completes  , total_layer  , building_lamination )values ('
    sqlinsert = crSQL(sql,result,tableName)
    conn.cursor()
    conn.text_factory = str # insert chinese
    conn.executemany(sqlinsert, result)
    # Show(conn,tableName)



def TEPMain(infoFile,nu):
    result = []
    titleName = []
    for line in infoFile:
        # if nu == 100 : break
        if nu == 0:
            title = line.split(',')
            for getTitle in title:  # get title name
                titleName.append(getTitle)
        if nu != 0:
            try:
                subData = line.split(',')

                districtTfInt = TransferDecodeEncode(subData[0])
                district = int(getCategory(titleName,districtTfInt,0))

                transaction = TransferDecodeEncode(subData[1])
                plate = TransferDecodeEncode(subData[2])
                if TransferDecodeEncode(subData[3]) == '':
                    squareMeter = None
                else:
                    squareMeter = float(TransferDecodeEncode(subData[3]))

                zoningTfInt = TransferDecodeEncode(subData[4])
                zoning = int(getCategory(titleName,zoningTfInt,4))

                non_metropolis = TransferDecodeEncode(subData[5])
                non_metropolis_land_use = TransferDecodeEncode(subData[6])
                transaction_year = TransferDecodeEncode(subData[7])
                month_and_day = TransferDecodeEncode(subData[8])
                transaction_pen = TransferDecodeEncode(subData[9])
                shifting_level = TransferDecodeEncode(subData[10])

                total_floorTfInt = TransferDecodeEncode(subData[11])
                total_floot = int(getCategory(titleName,total_floorTfInt,11))

                building_stateTfInt = TransferDecodeEncode(subData[12])
                building_state = int(getCategory(titleName,building_stateTfInt,12))

                main_useTfInt = TransferDecodeEncode(subData[13])
                main_use = int(getCategory(titleName,main_useTfInt,13))
                try:
                    main_building_materials = int(TransferDecodeEncode(subData[14]))
                    construction = float(TransferDecodeEncode(subData[15]))
                    building_shifting = int(TransferDecodeEncode(subData[16]))
                    building_present = int(TransferDecodeEncode(subData[17]))
                    building_present_hall =  int(TransferDecodeEncode(subData[18]))
                except:
                    main_building_materials = None
                    construction = None
                    building_shifting = None
                    building_present = None
                    building_present_hall = None

                buildingPresent_health = TransferDecodeEncode(subData[19])
                buildingPresent_hcompartmented =  TransferDecodeEncode(subData[20])
                try:
                    totail_money = int(TransferDecodeEncode(subData[21]))
                    total_price = int(TransferDecodeEncode(subData[22]))
                except:
                    totail_money = None
                    total_price = None

                unit_preiceTfInt = TransferDecodeEncode(subData[23])
                unit_preice = int(getCategory(titleName,unit_preiceTfInt,23))
                # print unit_preiceTfInt, unit_preice
                try:
                    berth_category = float(TransferDecodeEncode(subData[24]))
                    berth_category_total = int(TransferDecodeEncode(subData[25]))
                except:
                    berth_category = None
                    berth_category_total = None

                berth_total_price = TransferDecodeEncode(subData[26])


                list = (district, transaction,plate,squareMeter , zoning,non_metropolis,non_metropolis_land_use,transaction_year,month_and_day,\
                    transaction_pen,shifting_level,total_floot,building_state,main_use,main_building_materials,construction,building_shifting,building_present,\
                    building_present_hall,buildingPresent_health,buildingPresent_hcompartmented,totail_money,total_price,\
                    unit_preice,berth_category,berth_category_total,berth_total_price)
                result.append(list)
            except:
                # print nu
                print '-------english name not using and Special symbol-------' , subData[0] +',',  subData[1] +',',  subData[2] +',',  subData[3]
        nu = nu + 1

    print 'count:', len(result)
    print 'row:', len(result[0])

    # print result[0]
    tableName = 'tephouse'
    sql = 'insert into %s (district ,transaction_sign ,house_number ,total_area ,zoning  , non_metropolis_district  , non_metropolis ,transaction_year,month_and_day ,transaction_pen ,shifting_level  ,total_floor ,building_state ,main_use ,building_materials ,construction_complete  , shifting_total_area ,present_situation_pattern_room , present_situation_pattern_hall ,present_situation_pattern_health ,present_situation_pattern_compartmented ,total_price ,unit_price ,car_category ,car_area ,car_total_area ,notation  )values ('
    sqlinsert = crSQL(sql,result,tableName)
    conn.cursor()
    conn.text_factory = str # insert chinese
    conn.executemany(sqlinsert, result)
    # Show(conn,tableName)


def main(conn):

    year_number = range(2,8)
    season = range(1,5)
    # print year_number
    for year in year_number:
        for s in season:
            path = '/Users/Neil/Desktop/file/10%s_%s.zip' % (year,s)
            # print path
            try:
                zf = zipfile.ZipFile(path, 'r')
                nu = 0
                nameloop = ['A','B','C']
                for en in nameloop:
                    # print en
                    for info in zf.infolist():
                        infoFile = zf.open(info)
                        if infoFile.name.__contains__('A_LVR_LAND_'+en+'_PARK.CSV'):
                            print '------' ,infoFile.name, '------'
                            TEP_Park(infoFile, nu)

                        if infoFile.name.__contains__('A_LVR_LAND_'+en+'_LAND.CSV'):
                            print '------', infoFile.name, '------'
                            TPE_Land(infoFile,nu)

                        if infoFile.name.__contains__('A_LVR_LAND_'+en+'_BUILD.CSV'):
                            print '------', infoFile.name, '------'
                            TPE_Build(infoFile,nu)

                        if infoFile.name.__contains__('A_LVR_LAND_'+en+'.CSV'):
                            print '------', infoFile.name, '------'
                            TEPMain(infoFile, nu)
            except:
                print 'no data '
        break

def connectDB():
    conn = sqlite3.connect('transform.dbf')
    print "Opened database successfully"
    c = conn.cursor()
    c.execute('''create table if not exists park (id integer primary key autoincrement ,serial_number text , category int , price int , area int );''')
    # print "Park Table created successfully"
    c.execute('''create table if not exists build(id integer primary key autoincrement,serial_number text , room_age int ,building_shifting real ,main_us int ,main_building_materials text,construction_completes text , total_layer text , building_lamination text); ''')
    # print "build Table created successfully"
    c.execute('''create table if not exists land(id integer primary key autoincrement ,serial_number text,sector_position text,shifting_area real,zoning int);''')
    # print "land Table created successfully"
    c.execute('''create table if not exists tephouse(id integer primary key autoincrement ,district int,transaction_sign text,house_number text,total_area real,zoning int , non_metropolis_district text , non_metropolis text,transaction_year text,month_and_day text,transaction_pen text,shifting_level text ,total_floor int,building_state int,main_use int,building_materials int,construction_complete real , shifting_total_area int,present_situation_pattern_room int, present_situation_pattern_hall int,present_situation_pattern_health text,present_situation_pattern_compartmented text,total_price int,unit_price int,car_category int,car_area real,car_total_area int,notation text);''')
    print "conn successfully"

    conn.commit()
    return conn

if __name__ == '__main__':
    print('------ run application ------')
    conn = connectDB()
    main(conn)
    conn.close()
    print ('------ Done ------')

