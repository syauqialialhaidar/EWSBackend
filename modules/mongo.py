import pymongo
client = pymongo.MongoClient("localhost", 27017)
db = client.ews


# --------------------------------------------------------
# HELPER: FUNGSI UNTUK MEMBUAT FILTER ID FLEKSIBEL
# --------------------------------------------------------
def _create_id_filter(post_id, id_project=None):
    """Membuat filter kueri MongoDB yang fleksibel untuk mencari ID di 3 field."""
    if not post_id:
        return {}
    
    # Mencari ID di salah satu dari tiga field
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

# --------------------------------------------------------
# FUNGSI-FUNGSI UTAMA (dengan penambahan platform di addToWatchList)
# --------------------------------------------------------

def addRefreshData(data, refresh_id, id_project):
    return db.refresh.insert_one({
        "refresh_id": refresh_id,
        "id_project": id_project,
        **data
        }).inserted_id
        
# PERUBAHAN KRITIS DI SINI: MENERIMA PARAMETER 'platform'
def addToWatchList(post_id, topik, jam_publikasi, id_project, adder=1, platform=None):
    """Menambahkan atau mengupdate post ke Watch List."""
    
    query_filter = _create_id_filter(post_id, id_project)
    
    exist = db.watch_list.find_one(query_filter)
    
    if exist is None:
        # PERBAIKAN: Masukkan ID ke FIELD yang BENAR berdasarkan platform
        insert_data = {
            "refresh_count": 1, 
            "is_active": True, 
            "topik": [topik], 
            "timestamp_publikasi": jam_publikasi, 
            'id_project': id_project, 
            "status": []
        }
        
        # Tambahkan ID ke field yang sesuai
        if platform == 'twitter':
            insert_data['tweet_id'] = post_id
        elif platform == 'instagram':
            insert_data['post_id'] = post_id
        elif platform == 'tiktok':
            insert_data['id_video'] = post_id
        else: # Fallback, asumsikan Twitter jika platform tidak terdeteksi
            insert_data['tweet_id'] = post_id
            
        return db.watch_list.insert_one(insert_data).inserted_id
        
    else:
        # Jika post sudah ada, update count dan topik menggunakan filter fleksibel
        return db.watch_list.update_one(
            query_filter, 
            {"$inc": {"refresh_count": adder}, "$addToSet": { "topik" : topik}}
        ).modified_count

def setWatchListStatus(post_id, status, id_project):
    """Mengatur status aktif/nonaktif Watch List."""
    f = _create_id_filter(post_id, id_project) # Menggunakan filter fleksibel
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
    # Proyeksi mencakup semua ID
    return list(db.watch_list.find(f, projection={"tweet_id": True, "post_id": True, "id_video": True, "is_active": True}))

def getActiveWatchList(id_project):
    f = {"is_active": True}
    if id_project is not None:
        f["id_project"] = id_project
    # Proyeksi mencakup semua ID
    return db.watch_list.find(f, projection={"tweet_id": True, "post_id": True, "id_video": True, "timestamp_publikasi": True, "topik": True, "status": True, "refresh_count": True, "is_active": True})

def addReport(data):
    return db.report.insert_one(data).inserted_id

def getThreshold(id_project):
    # Asumsi Anda punya dokumen threshold berdasarkan id_project
    return db.threshold.find_one({"id_project": id_project})

def getDataWithTimestamp(timestamp, id_project):
    f = {"refresh_id": timestamp}
    if id_project is not None:
        f["id_project"] = id_project
    return db.rapidapi_alexander.find(f)

def getMinimumActiveTimestamp(id_project):
    f = {"is_active": True}
    if id_project is not None:
        f["id_project"] = id_project
    # Ini masih akan mengembalikan dokumen pertama berdasarkan timestamp
    return db.watch_list.find_one(f, sort=[("timestamp_publikasi", pymongo.ASCENDING)])

def appendStatusToWatchList(post_id, status, id_project):
    f = _create_id_filter(post_id, id_project) # Menggunakan filter fleksibel
    print(f, status)
    return db.watch_list.update_one(f, {"$push": {"status": status}}).modified_count

def getActionFrom3Status(s1,s2,s3):
    return db.actions.find_one({"s1": s1, "s2": s2, "s3": s3})

def addRequestCount(refresh_id, count, id_project):
    return db.refresh_count.insert_one({"refresh_id": refresh_id, "count": count, "id_project": id_project}).inserted_id