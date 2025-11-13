from fastapi import FastAPI, HTTPException, Query
from pymongo import MongoClient
from sshtunnel import SSHTunnelForwarder
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, date, time # <-- TAMBAHAN IMPORT
import pytz
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

app = FastAPI(title="EWS Statistics API")

# KONFIGURASI CORS BARU
origins = [
    "http://localhost",
    "http://localhost:5173",
    "http://127.0.0.1:8000"
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

SSH_CONFIG = {
    "SSH_HOST": os.getenv("SSH_HOST"),
    "SSH_PORT": int(os.getenv("SSH_PORT", 22)),
    "SSH_USER": os.getenv("SSH_USER"),
    "SSH_PASSWORD": os.getenv("SSH_PASSWORD"),
    "DB_HOST": os.getenv("DB_HOST"),
    "DB_PORT": int(os.getenv("DB_PORT", 27017)),
    "TARGET_DB": os.getenv("TARGET_DB")
}

def get_db_connection():
    try:
        server = SSHTunnelForwarder(
            (SSH_CONFIG["SSH_HOST"], SSH_CONFIG["SSH_PORT"]),
            ssh_username=SSH_CONFIG["SSH_USER"],
            ssh_password=SSH_CONFIG["SSH_PASSWORD"],
            remote_bind_address=(SSH_CONFIG["DB_HOST"], SSH_CONFIG["DB_PORT"])
        )
        server.start()
        
        client = MongoClient(f'mongodb://{SSH_CONFIG["DB_HOST"]}:{server.local_bind_port}/')
        db = client[SSH_CONFIG["TARGET_DB"]]
        return db, server
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {str(e)}")

# --- DEFINISI GLOBAL ---
WATCH_LIST_COLLECTION = "watch_list"
COLLECTION_NAME = "rapidapi_alexander"
STATUS_MAP = {0: "Normal", 1: "Early", 2: "Emerging", 3: "Current", 4: "Crisis"}


@app.get("/sentiment-distribution")
async def get_sentiment_distribution(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    """
    Get sentiment distribution (from watch_list) AND total unique posts (from rapidapi_alexander).
    """
    db, server = get_db_connection()
    try:
        # ==================================================================
        # BAGIAN 1: LOGIKA SENTIMEN (TIDAK BERUBAH)
        # Mengambil data dari 'watch_list' berdasarkan 'timestamp_publikasi'
        # ==================================================================
        match_filter_sentiment = {"status": {"$exists": True}}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter_sentiment["timestamp_publikasi"] = {
                "$gte": int(start_dt.timestamp()), 
                "$lte": int(end_dt.timestamp())
            }
            
        pipeline_sentiment = [
            {"$match": match_filter_sentiment},
            {"$unwind": "$status"},
            {"$group": {"_id": "$status", "count": {"$sum": 1}}}
        ]
        
        result_sentiment = list(db.watch_list.aggregate(pipeline_sentiment))
        sentiment_distribution = {"positive": 0, "negative": 0, "neutral": 0}
        
        for item in result_sentiment:
            status = int(item["_id"])
            count = item["count"]
            if status >= 3: sentiment_distribution["negative"] += count
            elif status == 0: sentiment_distribution["neutral"] += count
            else: sentiment_distribution["positive"] += count
        
        # ==================================================================
        # BAGIAN 2: LOGIKA TOTAL UNIQUE POSTS (BARU)
        # Mengambil data dari 'rapidapi_alexander' berdasarkan 'created_at'
        # ==================================================================
        pipeline_total = []
        match_filter_total = {}
        if start_date and end_date:
            start_dt_total = datetime.combine(start_date, time.min)
            end_dt_total = datetime.combine(end_date, time.max)
            # Perhatikan: ini menggunakan 'created_at' sesuai logika /total-unique-posts
            match_filter_total["created_at"] = {
                "$gte": int(start_dt_total.timestamp()), 
                "$lte": int(end_dt_total.timestamp())
            }
            pipeline_total.append({"$match": match_filter_total})
        
        pipeline_total.extend([
            {"$group": {"_id": "$tweet_id"}},
            {"$count": "total_unique_tweets"}
        ])

        result_cursor_total = list(db.rapidapi_alexander.aggregate(pipeline_total))
        
        total_unique = 0
        if result_cursor_total:
            total_unique = result_cursor_total[0].get("total_unique_tweets", 0)

        # ==================================================================
        # BAGIAN 3: RETURN (DIUBAH)
        # ==================================================================
        return {
            "sentiment_distribution": sentiment_distribution,
            # Menggunakan total_unique, BUKAN sum(sentiment_distribution.values())
            "total_posts": total_unique 
        }
    finally:
        server.close()


@app.get("/viral-posts")
async def get_viral_posts(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    """Get viral posts statistics with an optional date filter."""
    db, server = get_db_connection()
    STATUS_VIRAL = [1, 2, 3, 4]
    STATUS_MAP_REVERSE = {4: "crisis", 3: "current", 2: "emerging", 1: "early", 0: "normal"}
    try:
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["timestamp_publikasi"] = {"$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())}
        
        pipeline = []
        if match_filter:
            pipeline.append({"$match": match_filter})

        pipeline.extend([
            {"$addFields": {"latest_status": {"$arrayElemAt": ["$status", -1]}}},
            {"$group": {"_id": "$latest_status", "count": {"$sum": 1}}}
        ])
        
        result = list(db.watch_list.aggregate(pipeline))
        
        status_counts = {"crisis": 0, "current": 0, "emerging": 0, "early": 0, "normal": 0}
        total_posts_viral = 0
        for item in result:
            status = item["_id"]
            count = item["count"]
            if status in STATUS_VIRAL: total_posts_viral += count
            status_name = STATUS_MAP_REVERSE.get(status)
            if status_name: status_counts[status_name] = count
        return {"total_viral_posts": total_posts_viral, "by_status": status_counts}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    finally:
        server.close()


@app.get("/status-distribution")
async def get_status_distribution(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    """Get total posts by status with an optional date filter."""
    db, server = get_db_connection()
    try:
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["timestamp_publikasi"] = {"$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())}

        pipeline = []
        if match_filter:
            pipeline.append({"$match": match_filter})
            
        pipeline.extend([
            {"$unwind": "$status"},
            {"$group": {"_id": "$status", "count": {"$sum": 1}}}
        ])
        
        result = list(db.watch_list.aggregate(pipeline))
        status_map_labels = {"4": "crisis", "3": "current", "2": "emerging", "1": "early", "0": "normal"}
        
        return {
            "distribution": [
                {"status": status_map_labels.get(str(r["_id"]), r["_id"]), "count": r["count"]} 
                for r in result
            ]
        }
    finally:
        server.close()
   
# ... (kode server.py Anda yang lain) ...

@app.get("/top-topics")
async def get_top_topics(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    """
    Get top 3 topics (based on unique tweet count) and their top posts, 
    with an optional date filter.
    """
    db, server = get_db_connection()
    try:
        match_filter_main = {}
        match_filter_watchlist = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            start_ts, end_ts = int(start_dt.timestamp()), int(end_dt.timestamp())
            match_filter_main["created_at"] = {"$gte": start_ts, "$lte": end_ts}
            match_filter_watchlist["timestamp_publikasi"] = {"$gte": start_ts, "$lte": end_ts}

        pipeline_top_topics = []
        if match_filter_main:
            pipeline_top_topics.append({"$match": match_filter_main})
        
        # ======================================================
        # INI ADALAH BAGIAN YANG DIUBAH
        # ======================================================
        # Logika diubah untuk menghitung tweet_id unik per topik
        pipeline_top_topics.extend([
            # 1. Kelompokkan berdasarkan topik DAN tweet_id untuk mendapatkan unik
            {"$group": {
                "_id": {
                    "topik": "$topik",
                    "tweet_id": "$tweet_id"
                }
            }},
            # 2. Kelompokkan LAGI hanya berdasarkan topik, lalu hitung jumlah uniknya
            {"$group": {
                "_id": "$_id.topik",  # _id sekarang adalah nama topiknya
                "count": {"$sum": 1}   # Menghitung jumlah tweet_id unik
            }},
            # 3. Urutkan berdasarkan hitungan unik
            {"$sort": {"count": -1}},
            # 4. Ambil 3 teratas
            {"$limit": 3}
        ])
        # ======================================================
        # AKHIR DARI BAGIAN YANG DIUBAH
        # ======================================================
        
        top_topics = list(db.rapidapi_alexander.aggregate(pipeline_top_topics))
        
        results = []
        for topic in top_topics:
            # Sisa dari fungsi ini tidak perlu diubah
            topic_keyword = topic["_id"]
            
            pipeline_top_posts = [{"$match": {"topik": topic_keyword}}]
            if match_filter_watchlist:
                pipeline_top_posts.append({"$match": match_filter_watchlist})
            pipeline_top_posts.extend([
                {"$addFields": {"latest_status_value": {"$arrayElemAt": ["$status", -1]}}},
                {"$match": {"latest_status_value": {"$ne": 0}}},
                {"$sort": {"timestamp_publikasi": -1}},
                {"$limit": 10},
                {"$project": {"tweet_id": 1, "latest_status_value": 1, "_id": 0}}
            ])
            top_10_posts_summary = list(db.watch_list.aggregate(pipeline_top_posts))

            top_posts_details = []
            for summary in top_10_posts_summary:
                post = db.rapidapi_alexander.find_one({"tweet_id": summary.get("tweet_id")})
                if post:
                    user_info = post.get("user_info", {})
                    top_posts_details.append({
                        "tweet_id": post.get("tweet_id"),
                        "latest_status": STATUS_MAP.get(summary.get("latest_status_value", 0), "Normal"),
                        "text": post.get("text"), "engagement": post.get("engagement"),
                        "created_at": datetime.fromtimestamp(post.get("created_at")),
                        "retweet_count": post.get("retweets"), "favorite_count": post.get("favorites"),
                        "reply_count": post.get("replies"), "url": f"https://twitter.com/user/status/{post.get('tweet_id')}",
                        "user": {"name": user_info.get("name"), "screen_name": user_info.get("screen_name"),
                                 "profile_image_url": user_info.get("avatar"), "followers_count": user_info.get("followers_count"),
                                 "following_count": user_info.get("friends_count")}
                    })
            results.append({"topic": topic_keyword, "total_posts": topic["count"], "top_10_posts": top_posts_details})
        return {"top_topics": results}
    finally:
        server.close()

# ... (sisa kode server.py Anda) ...

# --- UNMODIFIED ENDPOINT ---

@app.get("/topic-status/{topic}")
async def get_topic_status(topic: str, id_project: Optional[str] = Query(None, description="tier / id_project to select threshold")):
    """Get status distribution and engagement trends for a specific topic."""
    db, server = get_db_connection()
    try:
        resolved_tier = id_project
        if not resolved_tier:
            wl_doc = db.watch_list.find_one({"topik": topic}, projection={"id_project": 1})
            if wl_doc and wl_doc.get("id_project"): resolved_tier = wl_doc.get("id_project")
            else:
                ra_doc = db.rapidapi_alexander.find_one({"topik": topic}, projection={"id_project": 1})
                if ra_doc and ra_doc.get("id_project"): resolved_tier = ra_doc.get("id_project")

        threshold_doc = db.threshold.find_one({"tier": resolved_tier}) if resolved_tier else db.threshold.find_one({})
        if not threshold_doc: raise HTTPException(status_code=404, detail="Threshold (tier) not found")

        def normalize_threshold(doc):
            if not doc: return None
            if all(k in doc for k in ("early", "emerging", "current", "crisis")): return {k: doc[k] for k in ("early", "emerging", "current", "crisis")}
            if "threshold" in doc and isinstance(doc["threshold"], list):
                m = {}; [m.update(item) for item in doc["threshold"] if isinstance(item, dict)]; return {k: m.get(k) for k in ("early", "emerging", "current", "crisis")}
            return {k: doc.get(k) for k in ("early", "emerging", "current", "crisis")}

        thresh = normalize_threshold(threshold_doc)
        if not thresh or any(v is None for v in thresh.values()): raise HTTPException(status_code=500, detail="Invalid threshold format in DB")

        pipeline = [
            {"$match": {"topik": topic}}, {"$sort": {"refresh_id": 1}},
            {"$group": {"_id": "$tweet_id", "engagements": {"$push": "$engagement"}, "timestamps": {"$push": "$refresh_id"}}}
        ]
        tweets = list(db.rapidapi_alexander.aggregate(pipeline))
        results = []
        total_current, total_prev = 0, 0
        for t in tweets:
            engs = t.get("engagements", [])
            if len(engs) < 2: continue
            prev, curr = engs[-2] or 0, engs[-1] or 0
            total_prev += prev; total_current += curr
            growth_percent = ((curr - prev) / (prev + 1)) * 100
            status_label = "Normal"
            if growth_percent > thresh["crisis"]: status_label = "Crisis"
            elif growth_percent > thresh["current"]: status_label = "Current"
            elif growth_percent > thresh["emerging"]: status_label = "Emerging"
            elif growth_percent > thresh["early"]: status_label = "Early"
            results.append({"tweet_id": t["_id"], "previous_engagement": prev, "current_engagement": curr, "growth_percentage": round(growth_percent, 2), "status": status_label})

        status_distribution = {}
        for r in results: status_distribution[r["status"]] = status_distribution.get(r["status"], 0) + 1
        
        topic_growth = ((total_current - total_prev) / (total_prev + 1)) * 100
        topic_status = "Normal"
        if topic_growth > thresh["crisis"]: topic_status = "Crisis"
        elif topic_growth > thresh["current"]: topic_status = "Current"
        elif topic_growth > thresh["emerging"]: topic_status = "Emerging"
        elif topic_growth > thresh["early"]: topic_status = "Early"

        return {"topic": topic, "resolved_id_project": resolved_tier, "threshold_used": thresh, "topic_status": topic_status, "topic_growth_percentage": round(topic_growth, 2), "status_distribution": status_distribution, "tweet_details": results}
    finally:
        server.close()

# --- MODIFIED ENDPOINTS (CONTINUED) ---

@app.get("/analysis-summary")
async def get_analysis_summary(
    topic: str = Query("all"),
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    """Get the count of posts for each status for a topic, with an optional date filter."""
    db, server = get_db_connection()
    try:
        match_filter = {}
        if topic != "all": match_filter["topik"] = topic
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["timestamp_publikasi"] = {"$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())}
            
        pipeline = [
            {"$match": match_filter},
            {"$addFields": {"latest_status_value": {"$arrayElemAt": ["$status", -1]}}},
            {"$group": {"_id": "$latest_status_value", "count": {"$sum": 1}}}
        ]
        status_counts = list(db.watch_list.aggregate(pipeline))
        summary_data = { "normal": 0, "early": 0, "emerging": 0, "current": 0, "crisis": 0 }
        status_map_keys = {0: "normal", 1: "early", 2: "emerging", 3: "current", 4: "crisis"}
        for item in status_counts:
            status_key = status_map_keys.get(item["_id"])
            if status_key: summary_data[status_key] = item["count"]
        return summary_data
    finally:
        server.close()
        

# GANTI FUNGSI LAMA ANDA DENGAN YANG INI
@app.get("/topic-trend-analysis")
async def get_topic_trend_analysis(
    topic: str = Query("all"),
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    """
    Get topic trend data.
    - Groups by day for date ranges > 1 day.
    - Groups by hour for a single day range.
    """
    db, server = get_db_connection()
    try:
        match_filter = {}
        if topic != "all":
            match_filter["topik"] = topic

        # Validasi dan set default jika tanggal tidak ada
        if not start_date or not end_date:
            today = datetime.now(pytz.timezone('Asia/Jakarta')).date()
            start_date = end_date = today

        start_dt = datetime.combine(start_date, time.min)
        end_dt = datetime.combine(end_date, time.max)
        match_filter["timestamp_publikasi"] = {
            "$gte": int(start_dt.timestamp()), 
            "$lte": int(end_dt.timestamp())
        }

        pipeline_base = [
            {"$match": match_filter},
            {"$addFields": {
                "latest_status_value": {"$arrayElemAt": ["$status", -1]}
            }},
        ]

        # ======================================================
        # INI ADALAH LOGIKA UTAMA (IF/ELSE)
        # ======================================================
        if start_date == end_date:
            # KASUS 1: Rentang 1 hari -> Agregasi per JAM
            group_stage = {
                "$group": {
                    "_id": {
                        "hour": {
                            "$dateToString": {
                                "format": "%H:00", 
                                "date": {"$toDate": {"$multiply": ["$timestamp_publikasi", 1000]}},
                                "timezone": "Asia/Jakarta" # <-- Cara timezone yang lebih baik
                            }
                        },
                        "status": "$latest_status_value"
                    },
                    "count": {"$sum": 1}
                }
            }
            labels = [f"{h:02d}:00" for h in range(24)]
            reshape_key_name = "hour"

        else:
            # KASUS 2: Rentang > 1 hari -> Agregasi per HARI
            group_stage = {
                "$group": {
                    "_id": {
                        "day": {
                            "$dateToString": {
                                "format": "%d-%m-%Y", # Format tanggal bisa diubah sesuai selera
                                "date": {"$toDate": {"$multiply": ["$timestamp_publikasi", 1000]}},
                                "timezone": "Asia/Jakarta"
                            }
                        },
                        "status": "$latest_status_value"
                    },
                    "count": {"$sum": 1}
                }
            }
            labels = []
            current_date = start_date
            while current_date <= end_date:
                labels.append(current_date.strftime("%d-%m-%Y"))
                current_date += timedelta(days=1)
            reshape_key_name = "day"

        # Gabungkan pipeline dan jalankan agregasi
        full_pipeline = pipeline_base + [group_stage]
        results = list(db.watch_list.aggregate(full_pipeline))

        # --- Proses Reshaping Data untuk Chart.js ---
        # (STATUS_MAP sudah ada di global, kita gunakan itu)
        
        data_points = {label: {cat: 0 for cat in STATUS_MAP.values()} for label in labels}

        for item in results:
            label = item["_id"][reshape_key_name]
            status_key = STATUS_MAP.get(item["_id"].get("status"))
            if label in data_points and status_key:
                data_points[label][status_key] = item["count"]

        datasets = []
        for status_name in STATUS_MAP.values():
            dataset = {
                "label": status_name,
                "data": [data_points[label][status_name] for label in labels]
            }
            datasets.append(dataset)

        return {"labels": labels, "datasets": datasets}
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    finally:
        server.close()
        
def format_post_output(post: Dict[str, Any], status_map: Dict[int, str]) -> Dict[str, Any]:
    numeric_status = post.get("latest_status_value", 0)
    latest_status = status_map.get(numeric_status, "N/A")
    created_at_ts = post.get("created_at")
    created_at_iso = datetime.fromtimestamp(created_at_ts, tz=pytz.utc).isoformat() if isinstance(created_at_ts, (int, float)) else None
    user_info = post.get("user_info", {})
    return {
        "tweet_id": post.get("tweet_id"), "text": post.get("text"), "engagement": post.get("engagement"),
        "created_at": created_at_iso, "latest_status": latest_status, "topik": post.get("topik"),
        "retweet_count": post.get("retweets"), "favorite_count": post.get("favorites"), "reply_count": post.get("replies"),
        "url": f"https://twitter.com/user/status/{post.get('tweet_id')}",
        "user": {"name": user_info.get("name"), "screen_name": user_info.get("screen_name"), "profile_image_url": user_info.get("avatar"),
                 "followers_count": user_info.get("followers_count"), "following_count": user_info.get("friends_count")}
    }

@app.get("/posts-by-engagement")
async def get_posts_by_engagement(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=200), skip: int = Query(0, ge=0)
):
    """Get posts sorted by highest engagement, with pagination and date filter."""
    db, server = get_db_connection()
    try:
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["created_at"] = {"$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())}
        
        pipeline = []
        if match_filter:
            pipeline.append({"$match": match_filter})
        
        pipeline.extend([
            {"$addFields": {"total_engagement_score": {"$sum": ["$favorites", "$replies", "$retweets"]}}},
            {"$sort": {"total_engagement_score": -1}},
            {"$group": {"_id": "$tweet_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"total_engagement_score": -1}},
            {"$lookup": {"from": WATCH_LIST_COLLECTION, "localField": "tweet_id", "foreignField": "tweet_id", "as": "status_info"}},
            {"$unwind": {"path": "$status_info", "preserveNullAndEmptyArrays": True}},
            {"$skip": skip}, {"$limit": limit},
            {"$project": {"tweet_id": 1, "text": 1, "engagement": "$total_engagement_score", "created_at": 1, "topik": 1,
                          "retweets": 1, "favorites": 1, "replies": 1, "latest_status_value": {"$arrayElemAt": ["$status_info.status", -1]},
                          "user_info": 1}}
        ])
        
        post_list = list(db[COLLECTION_NAME].aggregate(pipeline, allowDiskUse=True))
        formatted_posts = [format_post_output(post, STATUS_MAP) for post in post_list]
        return {"posts_by_engagement": formatted_posts}
    finally:
        server.close()

@app.get("/posts-by-followers")
async def get_posts_by_followers(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=200), skip: int = Query(0, ge=0)
):
    """Get posts from unique users sorted by followers, with pagination and date filter."""
    db, server = get_db_connection()
    try:
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["created_at"] = {"$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())}
            
        pipeline = []
        if match_filter:
            pipeline.append({"$match": match_filter})
        
        pipeline.extend([
            {"$group": {"_id": "$tweet_id", "doc": {"$last": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"user_info.followers_count": -1}},
            {"$group": {"_id": "$user_info.screen_name", "best_post_per_user": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$best_post_per_user"}},
            {"$sort": {"user_info.followers_count": -1}},
            {"$lookup": {"from": WATCH_LIST_COLLECTION, "localField": "tweet_id", "foreignField": "tweet_id", "as": "status_info"}},
            {"$unwind": {"path": "$status_info", "preserveNullAndEmptyArrays": True}},
            {"$skip": skip}, {"$limit": limit},
            {"$project": {"tweet_id": 1, "text": 1, "engagement": 1, "created_at": 1, "topik": 1,
                          "retweets": 1, "favorites": 1, "replies": 1, "latest_status_value": {"$arrayElemAt": ["$status_info.status", -1]},
                          "user_info": 1}}
        ])
        
        post_list = list(db[COLLECTION_NAME].aggregate(pipeline, allowDiskUse=True))
        formatted_posts = [format_post_output(post, STATUS_MAP) for post in post_list]
        return {"posts_by_followers": formatted_posts}
    finally:
        server.close()




# DATA ENDPOINTS BARU



@app.get("/total-unique-posts")
async def get_total_unique_posts(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    """
    Get the total count of UNIQUE posts scraped, with an optional date filter.
    Counts distinct tweet_ids from the raw scrape log.
    """
    db, server = get_db_connection()
    try:
        pipeline = []
        
        # 1. Filter Tanggal (Opsional)
        #    Ini akan memfilter berdasarkan KAPAN tweet itu di-posting (created_at)
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            # Menggunakan 'created_at' dari koleksi rapidapi_alexander
            match_filter["created_at"] = {
                "$gte": int(start_dt.timestamp()), 
                "$lte": int(end_dt.timestamp())
            }
            pipeline.append({"$match": match_filter})
        
        # 2. Logika Inti: Menghitung Tweet Unik
        #    Tahap 1: Kelompokkan semua tweet berdasarkan "tweet_id"
        pipeline.append({
            "$group": {
                "_id": "$tweet_id"
            }
        })
        
        #    Tahap 2: Hitung jumlah kelompok yang terbentuk
        pipeline.append({
            "$count": "total_unique_tweets"
        })

        # 3. Jalankan Agregasi
        result_cursor = list(db.rapidapi_alexander.aggregate(pipeline))
        
        # 4. Format Hasil
        if not result_cursor:
            # Jika tidak ada data sama sekali
            return {"total_unique_posts": 0}
            
        total = result_cursor[0].get("total_unique_tweets", 0)
        return {"total_unique_posts": total}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    finally:
        server.close()


@app.get("/all-unique-topics")
async def get_all_unique_topics():
    """
    Mengambil daftar semua topik unik (keyword) beserta id_project-nya dari koleksi rapidapi_alexander.
    """
    db, server = get_db_connection()
    try:
        pipeline = []
        
        # Tambahkan filter id_project jika disediakan
        # if id_project:
        #      pipeline.append({"$match": {"id_project": id_project}})
        
        # Pipeline Agregasi:
        pipeline.extend([
            # 1. Grouping berdasarkan kombinasi topik dan id_project
            #    Ini memastikan setiap baris output adalah kombinasi unik dari topik dan id_project
            {"$group": {"_id": {"topik": "$topik", "id_project": "$id_project"}}},
            
            # 2. Proyeksi untuk merapikan output
            {"$project": {
                "_id": 0,
                "topik": "$_id.topik",
                "id_project": "$_id.id_project"
            }},
            
            # 3. Pengurutan untuk keterbacaan
            {"$sort": {"topik": 1, "id_project": 1}}
        ])

        results = list(db.rapidapi_alexander.aggregate(pipeline))
        
        # Filter nilai topik yang None (jika ada)
        unique_topics_with_project = [
            item for item in results 
            if item.get("topik") is not None
        ] 
        
        return {"topics": unique_topics_with_project}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching unique topics: {str(e)}")
    finally:
        if server:
            server.close()


@app.get("/threshold/{id_project}")
async def get_threshold_by_id(id_project: str):
    """
    Mengambil data threshold dari koleksi 'threshold' berdasarkan id_project (tier).
    """
    db, server = get_db_connection()
    try:
        # Panggil fungsi getThreshold dari mongo.py
        # Menggunakan db.threshold.find_one
        threshold_doc = db.threshold.find_one({"tier": id_project})
        
        if not threshold_doc:
            # Jika tidak ditemukan, kembalikan data default atau kosong
            # Frontend akan menginisialisasi grid dengan nilai kosong jika ini terjadi.
            return {"tier": id_project, "threshold": []} 
        
        # Hapus _id dari hasil sebelum dikirim
        if "_id" in threshold_doc:
            del threshold_doc["_id"]
            
        return threshold_doc
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching threshold: {str(e)}")
    finally:
        server.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)