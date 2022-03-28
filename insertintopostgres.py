import json
import psyco
'''merge'''
f = open("unique_data.txt", "r")
data=json.load(f)
count=0


 
try:
    conn = psycopg2.connect("dbname='osmclone1' user='postgres' host='192.168.101.63' password='postgres'")
    conn.autocommit = True
except:
    print("cannot connect to db")

for data in data:
    """inserting """
    latitude=str(data ['latitude']*100000000)
    lat9digit= latitude[0:9]
    longitude=str(data ['longitude']*100000000)
    long9digit=longitude[0:9]

    cur = conn.cursor()
    sql =  "insert into nodes (node_id,latitude,longitude,changeset_id,visible,timestamp,tile,version,redaction_id) values(%s,%s,%s,6,True,'2021-06-03 09:11:38.545697' ,1,1,12);"
    cur.execute(sql,[data['id'],int(lat9digit),int(long9digit)])

    #node_tags
    if data['business_name'] != None and data['place_name'] == None:
        cur = conn.cursor()
        sql =  "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:place:en',%s);"
        cur.execute(sql,[data['id'],str(data['business_name'])])
    if data['business_name'] == None and data['place_name'] != None:
        cur = conn.cursor()
        sql =  "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:place:en',%s);"
        cur.execute(sql,[data['id'],str(data['place_name'])])
    if data['business_name'] != None and data['place_name'] != None:
        cur = conn.cursor()
        sql =  "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:place:en',%s);insert into node_tags  (node_id,version,k,v) values(%s,1,'business:name',%s);"
        cur.execute(sql,[data['id'],str(data['place_name']),data['id'],str(data['business_name'])])
    cur = conn.cursor()
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:housenumber',%s);"
    cur.execute(sql,[data['id'],str(data['holding_number'])])
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:street',%s);"
    cur.execute(sql, [data['id'], str(data['road_name_number'])])
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:suburb',%s);"
    cur.execute(sql, [data['id'], str(data['super_sub_area'])])
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'type',%s);"
    cur.execute(sql, [data['id'], str(data['pType'])])
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'rank',%s);"
    cur.execute(sql, [data['id'], str(data['popularity_ranking'])])
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:full',%s);"
    cur.execute(sql, [data['id'],  data['Address'] ])
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'area',%s);"
    cur.execute(sql, [data['id'], str(data['area'])])

    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:city',%s);"
    cur.execute(sql, [data['id'], str(data['city'])])
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:subdistrict',%s);"
    cur.execute(sql, [data['id'],str(data['sub_district'])])
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:district',%s);"
    cur.execute(sql, [data['id'], str(data['district'])])
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'addr:thana',%s);"
    cur.execute(sql, [data['id'],  str(data['thana']) ])
    sql= "insert into node_tags  (node_id,version,k,v) values(%s,1,'source','Tashfiqul Ghani & Rubayet Joy');"
    cur.execute(sql, [data['id']])









    count += 1
    print(count)
    print(data['id'])

     
'''
try:
    conn = psycopg2.connect("dbname='osmclone1' user='postgres' host='192.168.101.63' password='postgres'")
    conn.autocommit = True
except:
    print("cannot connect to db")
 
cur = conn.cursor()
sql="insert into nodes (node_id,latitude,longitude,changeset_id,visible,tile,version,redaction_id) values({},{},{},6,True ,1,1,12);".format(data[0][0]['id'],int(lat9digit),int(long9digit))
print(sql)


cur.execute(sql)
 '''
'''
print(data[0][0]['Address'])
print(data[0][0]['business_name'])
print(data[0][0]['place_name'])
print(data[0][0]['holding_number'])
print(data[0][0]['road_name_number'])
print(data[0][0]['pType'])
print(data[0][0]['area'])
print(data[0][0]['city'])
print(data[0][0]['sub_district'])
print(data[0][0]['district'])
print(data[0][0]['thana'])
'''

