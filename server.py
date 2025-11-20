from fastapi import FastAPI, HTTPException, Query
from pymongo import MongoClient
from sshtunnel import SSHTunnelForwarder
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta, date, time
import pytz
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv
import os

load_dotenv()
app = FastAPI(title="EWS Statistics API - Multi-Platform Ready")
origins = [
    "http://154.26.134.72:8439",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

DB_CONFIG = {
    "DB_HOST": os.getenv("DB_HOST", "localhost"),
    "DB_PORT": int(os.getenv("DB_PORT", 27018)),
    "TARGET_DB": os.getenv("TARGET_DB", "ewsnew")
}

WATCH_LIST_COLLECTION = "watch_list"
COLLECTION_NAME = "rapidapi_alexander"
STATUS_MAP = {0: "Normal", 1: "Early", 2: "Emerging", 3: "Current", 4: "Crisis"}
JAKARTA_TZ = pytz.timezone('Asia/Jakarta')

def get_db_connection():
    try:
        uri = f'mongodb://{DB_CONFIG["DB_HOST"]}:{DB_CONFIG["DB_PORT"]}/'
        client = MongoClient(uri, serverSelectionTimeoutMS=5000)
        client.admin.command('ping') 
        db = client[DB_CONFIG["TARGET_DB"]]
        return db, None 
    except Exception as e:
        error_msg = f"Database connection error: Failed to connect to {DB_CONFIG['DB_HOST']}:{DB_CONFIG['DB_PORT']} - {str(e)}"
        print(error_msg)
        raise HTTPException(status_code=500, detail=error_msg)

def clean_topic_name(topic_keyword: str) -> str:
    if not topic_keyword:
        return topic_keyword

    if topic_keyword.startswith('\\"') and topic_keyword.endswith('\\"'):
        return topic_keyword[2:-2] 

    if topic_keyword.startswith('"') and topic_keyword.endswith('"'):
        return topic_keyword[1:-1] 

    return topic_keyword

def get_canonical_id_pipeline(field_name: str = "_id"):
    return {
        "$addFields": {
            field_name: {
                "$ifNull": [
                    "$post_id", {"$ifNull": ["$id_video", "$tweet_id"]}
                ]
            }
        }
    }

def get_engagement_score_pipeline():
    return {
        "$addFields": {
            "total_engagement_score": {
                "$sum": [
                    {"$ifNull": ["$favorites", 0]}, 
                    {"$ifNull": ["$replies", 0]},    
                    {"$ifNull": ["$retweets", 0]},  
                    {"$ifNull": ["$likes", 0]},      
                    {"$ifNull": ["$comments", 0]},   
                    {"$ifNull": ["$reposts", 0]},    
                    {"$ifNull": ["$share", 0]},      
                    {"$ifNull": ["$views", 0]},      
                    {"$ifNull": ["$digg", 0]},       
                    {"$ifNull": ["$coments", 0]},    
                    {"$ifNull": ["$engagement", 0]}, 
                ]
            }
        }
    }


def get_canonical_id_pipeline(field_name: str = "_id"):
    return {
        "$addFields": {
            field_name: {
                "$ifNull": [
                    "$post_id", {"$ifNull": ["$id_video", "$tweet_id"]}
                ]
            }
        }
    }

def format_post_output(post: Dict[str, Any], status_map: Dict[int, str]) -> Dict[str, Any]:
    numeric_status = post.get("latest_status_value", 0)
    latest_status = status_map.get(numeric_status, "N/A")
    created_at_ts = post.get("created_at")
    created_at_dt = datetime.fromtimestamp(created_at_ts, tz=pytz.utc).astimezone(JAKARTA_TZ) if isinstance(created_at_ts, (int, float)) else None
    post_id = post.get("tweet_id") or post.get("post_id") or post.get("id_video")
    platform = post.get("platform", "N/A") 
    url = "N/A"
    user_info = post.get("user_info", {})

    if platform == 'instagram' and post_id:
        url = f"https://www.instagram.com/p/{post_id}/"
    elif platform == 'tiktok' and post_id:
        tiktok_username = user_info.get("screen_name") or post.get("username") or "v" 
        url = f"https://www.tiktok.com/@{tiktok_username}/video/{post_id}"
        if tiktok_username == 'v':
            url = f"https://www.tiktok.com/v/{post_id}" 
    elif platform == 'twitter' and post_id:
        url = f"https://twitter.com/user/status/{post_id}"
    
    followers = user_info.get("followers_count") or post.get("followerCount") or 0 
    following = user_info.get("friends_count") or post.get("followingCount") or 0
    text_content = post.get("text") or post.get("captions") or post.get("capstion") 
    
    specific_metrics = {
        "twitter": {"retweets": post.get("retweets"), "favorites": post.get("favorites"), "replies": post.get("replies")},
        "instagram": {"likes": post.get("likes"), "comments": post.get("comments"), "reposts": post.get("reposts"), "shares": post.get("share")},
        "tiktok": {"views": post.get("views"), "likes": post.get("like"), "comments": post.get("coments"), "shares": post.get("share")},
    }
    
    return {
        "post_id": post_id,
        "platform": platform,
        "text_content": text_content,
        "engagement": post.get("total_engagement_score", post.get("engagement")),
        "created_at": created_at_dt.isoformat() if created_at_dt else None,
        "latest_status": latest_status,
        "topik": clean_topic_name(post.get("topik", "")),  # Clean topic name
        "url": url,
        "metrics_detail": specific_metrics,
        "user": {
            "name": user_info.get("name") or post.get("nickname"), 
            "screen_name": user_info.get("screen_name") or post.get("username"), 
            "profile_image_url": user_info.get("avatar") or post.get("foto_profil") or post.get("profil"),
            "followers_count": followers, 
            "following_count": following
        }
    }

@app.get("/sentiment-distribution")
async def get_sentiment_distribution(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    db, server = get_db_connection()
    try:
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["created_at"] = {
                "$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())
            }
        match_filter["sentiment"] = {"$in": ["Positive", "Negative", "Neutral"]}

        pipeline_sentiment = [
            {"$match": match_filter},
            get_canonical_id_pipeline("canonical_id"),
            {"$group": {"_id": "$canonical_id", "sentiment": {"$last": "$sentiment"}}},
            {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}
        ]
        result_sentiment = list(db.rapidapi_alexander.aggregate(pipeline_sentiment))
        sentiment_distribution = {"positive": 0, "negative": 0, "neutral": 0}

        for item in result_sentiment:
            sentiment_label = item["_id"].lower()
            if sentiment_label in sentiment_distribution:
                sentiment_distribution[sentiment_label] = item["count"]

        total_unique = sum(sentiment_distribution.values())
        return {"sentiment_distribution": sentiment_distribution, "total_posts": total_unique}
    finally:
        if server: server.close()

@app.get("/total-unique-posts")
async def get_total_unique_posts(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    db, server = get_db_connection()
    try:
        pipeline = []
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["timestamp_publikasi"] = { 
                "$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())
            }
            pipeline.append({"$match": match_filter})
        
        pipeline.append({
             "$addFields": {
                 "canonical_id": {
                     "$ifNull": [
                         "$tweet_id", {"$ifNull": ["$post_id", "$id_video"]}
                     ]
                 }
             }
        })

        pipeline.append({"$group": {"_id": "$canonical_id"}})
        pipeline.append({"$count": "total_unique_posts"})
        result_cursor = list(db[WATCH_LIST_COLLECTION].aggregate(pipeline)) 
        total = result_cursor[0].get("total_unique_posts", 0) if result_cursor else 0
        return {"total_unique_posts": total}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    finally:
        if server: server.close()

@app.get("/top-topics")
async def get_top_topics(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
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

        pipeline_all_topics = []
        if match_filter_main: pipeline_all_topics.append({"$match": match_filter_main})
        pipeline_all_topics.append(get_canonical_id_pipeline("canonical_id"))

        pipeline_all_topics.extend([
            {"$group": {
                "_id": {"topik": "$topik", "canonical_id": "$canonical_id"}
            }},
            {"$group": {
                "_id": "$_id.topik",  
                "count": {"$sum": 1} 
            }},
            {"$sort": {"count": -1}},
        ])
        all_topics = list(db.rapidapi_alexander.aggregate(pipeline_all_topics))
        
        results = []
        for topic in all_topics:
            topic_keyword = topic["_id"]
            if topic_keyword is None:
                continue

            topic_name = clean_topic_name(topic_keyword)

            pipeline_top_posts = [{"$match": {"topik": topic_keyword}}]
            if match_filter_watchlist: pipeline_top_posts.append({"$match": match_filter_watchlist})
            
            pipeline_top_posts.extend([
                {"$addFields": {"canonical_id": {"$ifNull": ["$tweet_id", {"$ifNull":["$post_id","$id_video"]}]}}},
                {"$addFields": {"latest_status_value": {"$arrayElemAt": ["$status", -1]}}},
                {"$sort": {"timestamp_publikasi": -1}}, 
                {"$limit": 10},
                {"$project": {"canonical_id": 1, "latest_status_value": 1, "_id": 0}}
            ])
            top_10_posts_summary = list(db.watch_list.aggregate(pipeline_top_posts))

            top_posts_details = []
            for summary in top_10_posts_summary:
                canonical_id = summary.get("canonical_id")
                post_match_filter = {
                    "$or": [
                        {"tweet_id": canonical_id},
                        {"post_id": canonical_id},
                        {"id_video": canonical_id}
                    ]
                }
                post = list(db.rapidapi_alexander.aggregate([
                    {"$match": post_match_filter},
                    get_engagement_score_pipeline(), 
                    {"$sort": {"total_engagement_score": -1}}, 
                    {"$limit": 1}
                ]))
                
                if post:
                    formatted_post = format_post_output({
                        **post[0],
                        "latest_status_value": summary.get("latest_status_value")
                    }, STATUS_MAP)
                    top_posts_details.append(formatted_post)

            results.append({
                "topic": topic_name,  
                "keyword": topic_keyword, 
                "total_unique_posts": topic["count"],
                "top_10_posts": top_posts_details
            })
        
        return {"all_topics_with_top_posts": results}
    finally:
        if server: server.close()


@app.get("/posts-by-engagement")
async def get_posts_by_engagement(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=200), skip: int = Query(0, ge=0)
):
    db, server = get_db_connection()
    try:
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["created_at"] = {"$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())}
        
        pipeline = []
        if match_filter: pipeline.append({"$match": match_filter})
        
        pipeline.extend([
            get_engagement_score_pipeline(), 
            get_canonical_id_pipeline("canonical_id"),
            {"$sort": {"total_engagement_score": -1}},
            {"$group": {"_id": "$canonical_id", "doc": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"total_engagement_score": -1}},
            {"$lookup": {
                "from": WATCH_LIST_COLLECTION,
                "let": {"cid": "$canonical_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$or": [
                        {"$eq": ["$tweet_id", "$$cid"]},
                        {"$eq": ["$post_id", "$$cid"]},
                        {"$eq": ["$id_video", "$$cid"]}
                    ]}}}
                ],
                "as": "status_info"
            }},
            {"$unwind": {"path": "$status_info", "preserveNullAndEmptyArrays": True}},
            {"$skip": skip}, {"$limit": limit},
            {"$project": {
                "tweet_id": 1, "post_id": 1, "id_video": 1, "platform": 1, "text": 1, "captions": 1, "capstion": 1,
                "engagement": "$total_engagement_score", "created_at": 1, "topik": 1,
                "retweets": 1, "favorites": 1, "replies": 1, "likes": 1, "comments": 1, "reposts": 1, "share": 1,
                "views": 1, "digg": 1, "coments": 1,
                "latest_status_value": {"$arrayElemAt": ["$status_info.status", -1]},
                "user_info": 1, "followerCount": 1, "followingCount": 1, "username": 1, "nickname": 1, "profil": 1, "foto_profil": 1
            }}
        ])
        
        post_list = list(db[COLLECTION_NAME].aggregate(pipeline, allowDiskUse=True))
        formatted_posts = [format_post_output(post, STATUS_MAP) for post in post_list]
        return {"posts_by_engagement": formatted_posts}
    finally:
        if server: server.close()

@app.get("/posts-by-followers")
async def get_posts_by_followers(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)"),
    limit: int = Query(100, ge=1, le=200), skip: int = Query(0, ge=0)
):
    db, server = get_db_connection()
    try:
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["created_at"] = {"$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())}
            
        pipeline = []
        if match_filter: pipeline.append({"$match": match_filter})
        pipeline.extend([
            {"$addFields": {
                "canonical_username": {"$ifNull": ["$user_info.screen_name", "$username"]},
                "canonical_followers_count": {"$ifNull": ["$user_info.followers_count", "$followerCount"]} 
            }},
            get_canonical_id_pipeline("canonical_id"),
            {"$group": {"_id": "$canonical_id", "doc": {"$last": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$doc"}},
            {"$sort": {"canonical_followers_count": -1}},
            {"$group": {"_id": "$canonical_username", "best_post_per_user": {"$first": "$$ROOT"}}},
            {"$replaceRoot": {"newRoot": "$best_post_per_user"}},
            {"$sort": {"canonical_followers_count": -1}},
            {"$lookup": {
                "from": WATCH_LIST_COLLECTION,
                "let": {"cid": "$canonical_id"},
                "pipeline": [
                    {"$match": {"$expr": {"$or": [
                        {"$eq": ["$tweet_id", "$$cid"]},
                        {"$eq": ["$post_id", "$$cid"]},
                        {"$eq": ["$id_video", "$$cid"]}
                    ]}}}
                ],
                "as": "status_info"
            }},
            {"$unwind": {"path": "$status_info", "preserveNullAndEmptyArrays": True}},
            {"$skip": skip}, {"$limit": limit},
            {"$project": {
                "tweet_id": 1, "post_id": 1, "id_video": 1, "platform": 1, "text": 1, "captions": 1, "capstion": 1,
                "engagement": 1, "created_at": 1, "topik": 1,
                "retweets": 1, "favorites": 1, "replies": 1, "likes": 1, "comments": 1, "reposts": 1, "share": 1,
                "views": 1, "digg": 1, "coments": 1,
                "latest_status_value": {"$arrayElemAt": ["$status_info.status", -1]},
                "user_info": 1, "followerCount": 1, "followingCount": 1, "username": 1, "nickname": 1, "profil": 1, "foto_profil": 1
            }}
        ])
        
        post_list = list(db[COLLECTION_NAME].aggregate(pipeline, allowDiskUse=True))
        formatted_posts = [format_post_output(post, STATUS_MAP) for post in post_list]
        return {"posts_by_followers": formatted_posts}
    finally:
        if server: server.close()

@app.get("/viral-posts")
async def get_viral_posts(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    db, server = get_db_connection()
    STATUS_VIRAL = [1, 2, 3, 4] 
    STATUS_MAP_REVERSE = {4: "crisis", 3: "current", 2: "emerging", 1: "early", 0: "normal"}
    try:
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["timestamp_publikasi"] = {"$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())}

        pipeline_status = []
        if match_filter: pipeline_status.append({"$match": match_filter})
        pipeline_status.extend([
            {"$addFields": {"latest_status": {"$arrayElemAt": ["$status", -1]}}},
            {"$group": {"_id": "$latest_status", "count": {"$sum": 1}}}
        ])
        result_status = list(db.watch_list.aggregate(pipeline_status))
        status_counts = {"crisis": 0, "current": 0, "emerging": 0, "early": 0, "normal": 0}
        total_posts_viral = 0
        for item in result_status:
            status = item["_id"]
            count = item["count"]
            if status in STATUS_VIRAL: total_posts_viral += count
            status_name = STATUS_MAP_REVERSE.get(status)
            if status_name: status_counts[status_name] = count

        pipeline_viral_ids = []
        if match_filter: pipeline_viral_ids.append({"$match": match_filter})
        pipeline_viral_ids.extend([
            {"$addFields": {"latest_status_value": {"$arrayElemAt": ["$status", -1]}}},
            {"$match": {"latest_status_value": {"$in": STATUS_VIRAL}}},
            {"$addFields": {
                "canonical_id": {
                    "$ifNull": ["$tweet_id", {"$ifNull": ["$post_id", "$id_video"]}]
                }
            }},
            {"$project": {"canonical_id": 1, "_id": 0}}
        ])
        viral_ids = list(db.watch_list.aggregate(pipeline_viral_ids))
        viral_id_list = [item["canonical_id"] for item in viral_ids if item.get("canonical_id")]

        sentiment_pipeline = [
            {
                "$match": {
                    "$or": [
                        {"tweet_id": {"$in": viral_id_list}},
                        {"post_id": {"$in": viral_id_list}},
                        {"id_video": {"$in": viral_id_list}}
                    ],
                    "sentiment": {"$in": ["Positive", "Negative", "Neutral"]}
                }
            },
            {
                "$addFields": {
                    "canonical_id": {
                        "$ifNull": ["$tweet_id", {"$ifNull": ["$post_id", "$id_video"]}]
                    }
                }
            },
            {"$group": {"_id": "$canonical_id", "sentiment": {"$last": "$sentiment"}}},
            {"$group": {"_id": "$sentiment", "count": {"$sum": 1}}}
        ]

        result_sentiment = list(db.rapidapi_alexander.aggregate(sentiment_pipeline))
        sentiment_distribution = {"positive": 0, "negative": 0, "neutral": 0}

        for item in result_sentiment:
            sentiment_label = item["_id"].lower()
            if sentiment_label in sentiment_distribution:
                sentiment_distribution[sentiment_label] = item["count"]

        total_sentiment_posts = sum(sentiment_distribution.values()) 
        return {
            "total_viral_posts": total_posts_viral,
            "by_status": status_counts,
            "sentiment_distribution": sentiment_distribution,
            "total_sentiment_posts": total_sentiment_posts
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    finally:
        if server: server.close()


@app.get("/status-distribution")
async def get_status_distribution(
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    db, server = get_db_connection()
    try:
        match_filter = {}
        if start_date and end_date:
            start_dt = datetime.combine(start_date, time.min)
            end_dt = datetime.combine(end_date, time.max)
            match_filter["timestamp_publikasi"] = {"$gte": int(start_dt.timestamp()), "$lte": int(end_dt.timestamp())}

        pipeline = []
        if match_filter: pipeline.append({"$match": match_filter})
            
        pipeline.extend([
            {"$addFields": {"latest_status_value": {"$arrayElemAt": ["$status", -1]}}},
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
        if server: server.close()
    
@app.get("/analysis-summary")
async def get_analysis_summary(
    topic: str = Query("all"),
    platform: str = Query("all"),
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    db, server = get_db_connection()
    try:
        match_filter = {}
        if topic != "all": match_filter["topik"] = topic
        if platform != "all":
            if platform == 'x' or platform == 'twitter': 
                match_filter["tweet_id"] = {"$exists": True, "$ne": None}
            elif platform == 'instagram':
                match_filter["post_id"] = {"$exists": True, "$ne": None}
            elif platform == 'tiktok':
                match_filter["id_video"] = {"$exists": True, "$ne": None}
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
        if server: server.close()
        
@app.get("/topic-trend-analysis")
async def get_topic_trend_analysis(
    topic: str = Query("all"),
    platform: str = Query("all"),
    start_date: Optional[date] = Query(None, description="Tanggal mulai (YYYY-MM-DD)"),
    end_date: Optional[date] = Query(None, description="Tanggal akhir (YYYY-MM-DD)")
):
    db, server = get_db_connection()
    try:
        match_filter = {}
        if topic != "all": match_filter["topik"] = topic

        if not start_date or not end_date:
            today = datetime.now(JAKARTA_TZ).date()
            start_date = end_date = today

        start_dt = datetime.combine(start_date, time.min)
        end_dt = datetime.combine(end_date, time.max)
        match_filter["timestamp_publikasi"] = {
            "$gte": int(start_dt.timestamp()), 
            "$lte": int(end_dt.timestamp())
        }

        pipeline_base = [
            {"$match": match_filter},
        ]
        
        platform_filter = None
        if platform != "all":
            if platform == 'x' or platform == 'twitter': 
                platform_filter = {"tweet_id": {"$exists": True, "$ne": None}}
            elif platform == 'instagram':
                platform_filter = {"post_id": {"$exists": True, "$ne": None}}
            elif platform == 'tiktok':
                platform_filter = {"id_video": {"$exists": True, "$ne": None}}
            if platform_filter:
                pipeline_base.append({"$match": platform_filter})

        pipeline_base.extend([
            {"$addFields": {
                "latest_status_value": {"$arrayElemAt": ["$status", -1]}
            }},
        ])

        if start_date == end_date:
            group_stage = {
                "$group": {
                    "_id": {
                        "hour": {
                            "$dateToString": {
                                "format": "%H:00", 
                                "date": {"$toDate": {"$multiply": ["$timestamp_publikasi", 1000]}},
                                "timezone": "Asia/Jakarta"
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
            group_stage = {
                "$group": {
                    "_id": {
                        "day": {
                            "$dateToString": {
                                "format": "%d-%m-%Y", 
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

        full_pipeline = pipeline_base + [group_stage]
        results = list(db.watch_list.aggregate(full_pipeline))
        data_points = {label: {cat: 0 for cat in STATUS_MAP.values()} for label in labels}

        for item in results:
            label = item["_id"][reshape_key_name]
            status_key = STATUS_MAP.get(item["_id"].get("status"))
            if label in data_points and status_key:
                data_points[label][status_key] = item["count"]

        datasets = []
        for status_name in STATUS_MAP.values():
            datasets.append({
                "label": status_name,
                "data": [data_points[label][status_name] for label in labels]
            })

        return {"labels": labels, "datasets": datasets}
    
    except Exception as e:
        print(f"Error in /topic-trend-analysis: {e}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")
    finally:
        if server: server.close()
        

@app.get("/all-unique-topics")
async def get_all_unique_topics():
    db, server = get_db_connection()
    try:
        pipeline = []
        pipeline.extend([
            {"$group": {"_id": {"topik": "$topik", "id_project": "$id_project"}}},
            {"$project": {
                "_id": 0,
                "topik": "$_id.topik",
                "id_project": "$_id.id_project"
            }},
            {"$sort": {"topik": 1, "id_project": 1}}
        ])

        results = list(db.rapidapi_alexander.aggregate(pipeline))
        unique_topics_with_project = [
            item for item in results 
            if item.get("topik") is not None
        ] 
        return {"topics": unique_topics_with_project}
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching unique topics: {str(e)}")
    finally:
        if server: server.close()


@app.get("/threshold/{id_project}")
async def get_threshold_by_id(id_project: str):
    db, server = get_db_connection()
    try:
        threshold_doc = db.threshold.find_one({"tier": id_project})
        
        if not threshold_doc:
            return {"tier": id_project, "threshold": []} 
        
        if "_id" in threshold_doc:
            del threshold_doc["_id"]
            
        return threshold_doc
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching threshold: {str(e)}")
    finally:
        if server: server.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8438)