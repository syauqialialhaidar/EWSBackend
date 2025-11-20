import pymongo
import os
from dotenv import load_dotenv

load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", 27017))
TARGET_DB = os.getenv("TARGET_DB", "ewsnew")

client = pymongo.MongoClient(DB_HOST, DB_PORT)
db = client[TARGET_DB]

def _create_id_filter(post_id, id_project=None):
    if not post_id:
        return {}
    
    f = {
        "$or": [
            {"tweet_id": post_id},
            {"post_id": post_id},
            {"id_video": post_id}
        ]
    }
    
    if id_project is not None:
        f["id_project"] = id_project
    
    return f

def addRefreshData(data, refresh_id, id_project):
    data_copy = data.copy() 
    if '_id' in data_copy:
        del data_copy['_id'] 
        
    return db.refresh.insert_one({
        "refresh_id": refresh_id,
        "id_project": id_project,
        **data_copy
    }).inserted_id
        

def addToWatchList(post_id, topik, jam_publikasi, id_project, adder=1, platform=None, sentiment=None):
    
    query_filter = _create_id_filter(post_id, id_project)
    exist = db.watch_list.find_one(query_filter)
    if exist is None:
        insert_data = {
            "refresh_count": 1, 
            "is_active": True, 
            "topik": [topik], 
            "timestamp_publikasi": jam_publikasi, 
            'id_project': id_project, 
            "status": [],
            "sentiment": sentiment if sentiment is not None else ""
        }
        
        if platform == 'twitter':
            insert_data['tweet_id'] = post_id
        elif platform == 'instagram':
            insert_data['post_id'] = post_id
        elif platform == 'tiktok':
            insert_data['id_video'] = post_id
        else:
            pass
            
        return db.watch_list.insert_one(insert_data).inserted_id
        
    else:
        update_operation = {
            "$inc": {"refresh_count": adder},
            "$addToSet": { "topik" : topik }
        }
        
        return db.watch_list.update_one(
            query_filter, 
            update_operation
        ).modified_count
    

def setWatchListStatus(post_id, status, id_project):
    f = _create_id_filter(post_id, id_project)
    return db.watch_list.update_one(f, {"$set": {"is_active": status}}).modified_count

def setAllWatchListStatus(status, id_project):
    f = {}
    if id_project is not None:
        f["id_project"] = id_project
    return db.watch_list.update_many(f, {"$set": {"is_active": status}}).modified_count
    
def queryRefreshAggregate(aggr):
    return db.refresh.aggregate(aggr)

def getWatchList(id_project):
    f = {}
    if id_project is not None:
        f["id_project"] = id_project
    return list(db.watch_list.find(f, projection={"tweet_id": True, "post_id": True, "id_video": True, "is_active": True}))

def getActiveWatchList(id_project):
    f = {"is_active": True}
    if id_project is not None:
        f["id_project"] = id_project
    return db.watch_list.find(f, projection={"tweet_id": True, "post_id": True, "id_video": True, "timestamp_publikasi": True, "topik": True, "status": True, "refresh_count": True, "is_active": True})

def addReport(data):
    return db.report.insert_one(data).inserted_id

def getThreshold(id_project):
    try:
        project_id_int = int(id_project)
    except (ValueError, TypeError):
        project_id_int = id_project 
        
    query_filter = {
        "$or": [
            {"tier": id_project},       
            {"tier": project_id_int}    
        ]
    }
    return db.threshold.find_one(query_filter)

def getDataWithTimestamp(timestamp, id_project):
    f = {"refresh_id": timestamp}
    if id_project is not None:
        f["id_project"] = id_project
    return db.rapidapi_alexander.find(f)

def getMinimumActiveTimestamp(id_project):
    f = {"is_active": True}
    if id_project is not None:
        f["id_project"] = id_project
    return db.watch_list.find_one(f, sort=[("timestamp_publikasi", pymongo.ASCENDING)])

def appendStatusToWatchList(post_id, status_code, id_project, sentiment=None):
    f = _create_id_filter(post_id, id_project) 
    
    update_op = {
        "$push": {"status": status_code} 
    }
    print(f, status_code)
    return db.watch_list.update_one(f, update_op).modified_count
def getActionFrom3Status(s1,s2,s3):
    return db.actions.find_one({"s1": s1, "s2": s2, "s3": s3})

def addRequestCount(refresh_id, count, id_project):
    return db.refresh_count.insert_one({"refresh_id": refresh_id, "count": count, "id_project": id_project}).inserted_id