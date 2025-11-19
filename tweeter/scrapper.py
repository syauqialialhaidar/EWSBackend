from datetime import date, datetime, timedelta
import pytz
import time
import os, sys
from dotenv import load_dotenv
from pathlib import Path
import requests
from pymongo import InsertOne, ReplaceOne 
from dateutil.parser import parse
from modules.mongo import addRequestCount, db, client, getMinimumActiveTimestamp 
import json
import traceback

load_dotenv()


def calculateEngagement(post):
    """Calculates a simple engagement score based on available metrics."""
    try:
        retweet = post.get('retweets', 0)
        favorite = post.get('favorites', 0)
        replies = post.get('replies', 0)
        return (retweet + favorite + replies)
    except:
        return 0

def GetKeywords(id_project=None):
    """Fetches keywords from the external Kurasi API."""
    if id_project is None:
        id_project = os.getenv("PROJECT_ID")

    url = f"https://api.kurasi.media/v2/crawler/get-keyword-all/medsos?id_project={id_project}"

    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            data = response.json()
            
            if isinstance(data, dict) and "keywords" in data:
                print(f"[KEYWORDS] Ditemukan {len(data['keywords'])} keyword untuk project {id_project}.")
                return {"keywords": data["keywords"]}  
            else:
                print("[KEYWORDS] Format respons API tidak sesuai.")
                return {"keywords": []}
        else:
            print(f"[KEYWORDS] Gagal ambil keyword (Status: {response.status_code}).")
            return {"keywords": []}

    except requests.exceptions.RequestException as e:
        print(f"[KEYWORDS] Request error: {e}")
        return {"keywords": []}


def getTimeWindow(start_timestamp, id_project):
    """Menentukan batas waktu minimum untuk crawling."""
    start_datetime = datetime.fromtimestamp(start_timestamp)
    
    if start_datetime.hour == 5:
        yesterday = start_datetime - timedelta(days=1)
        yesterdayAt9 = yesterday.replace(hour=20, minute=0, second=0)
        unix_time = int(time.mktime(yesterdayAt9.timetuple()))
        print(f"[TIME WINDOW] Start time is 5 AM, setting minimum timestamp to 20:00 yesterday ({unix_time}).")
        return None, unix_time

    v_minimum = getMinimumActiveTimestamp(id_project)

    if v_minimum:
        v_minimum_timestamp = v_minimum.get('timestamp_publikasi', start_timestamp - 3600)
        v_minimum_id = v_minimum.get('tweet_id')
    else:
        # Default 1 jam ke belakang jika tidak ada data di Mongo
        v_minimum_timestamp = start_timestamp - 60*60 
        v_minimum_id = None
        
    return v_minimum_id, v_minimum_timestamp


class ScriptFix:

    def __init__(self, args):
        self.StartTime = time.time()
        self.IST = pytz.timezone('Asia/Jakarta')
        self.datetime_ist = datetime.now(self.IST)
        self.args = args
        self.DatabaseConnection = client
        self.screen_result = {}
        self.db_collection_name = None
        self.api_function = None
        self.format_data_function = None 

    def __enter__(self):
        print("--------------------------------------------------------")
        print(" Glavier Social Media Search V2 (Refactored)")
        print("--------------------------------------------------------")
        print("Started Time:", self.datetime_ist)
        print("Args: {}".format(self.args.__dict__))
        self.db = db
        return self

    def GenerateSentiment(self, text_content):
        """Generates sentiment using the external Naive Bayes API."""
            
        # Increased timeout to 30s for stability 
        url = "https://generate-sentiment-naive-baiyes.onlinemonitoring.id/sentiment/generate"
        headers = {
            "Content-Type": "application/json"
        }
        payload = json.dumps({"data": [text_content]})
        
        try:
            response = requests.post(url, headers=headers, data=payload, timeout=30) 
            
            if response.status_code == 200:
                json_response = response.json()
                if isinstance(json_response, dict) and 'sentiments' in json_response and json_response.get('sentiments'):
                    sentiment_code = json_response['sentiments'][0]
                    
                    sentiment_map = {
                        -1: 'Negative',
                        0: 'Neutral', 
                        1: 'Positive',
                    }
                    
                    sentiment_label = sentiment_map.get(sentiment_code, 'UNKNOWN')
                    
                    return {
                        'sentiment': sentiment_label,
                        'confidence': 1.0, 
                        'source': 'sentiment_api_v3_parsed'
                    }

                else:
                    print(f"[SENTIMENT API] Success but Invalid response format: {json_response}")
                    return {"sentiment": "ERROR", "confidence": 0.0, "api_status": "Invalid format"}

            else:
                error_detail = response.text
                print(f"[SENTIMENT API] Error {response.status_code}: {error_detail[:200]}")
                return {"sentiment": "ERROR", "confidence": 0.0, "api_status": response.status_code}
        except requests.exceptions.RequestException as e:
            # Handles ReadTimeout and other connection errors gracefully
            print(f"[SENTIMENT API] Connection Error: {type(e).__name__} - {e}")
            return {"sentiment": "TIMEOUT", "confidence": 0.0, "api_error": str(e)}
        except Exception as e:
            print(f"[SENTIMENT API] Unexpected Error: {e}")
            return {"sentiment": "ERROR", "confidence": 0.0, "api_error": str(e)}

    def ConvertDate(self, date_str):
        """Converts date string (from Twitter) to Unix timestamp."""
        dt = parse(date_str)
        return int(dt.timestamp())

    def FormatTwitterData(self, tweet, refresh_id):
        """Formats Twitter data for MongoDB ingestion."""
        try:
            v_date = tweet['created_at']
            tweet['created_at'] = self.ConvertDate(v_date)
            tweet['refresh_id'] = refresh_id
            tweet['engagement'] = calculateEngagement(tweet)
            tweet['timestamp'] = int(time.time())
            # Ensure post ID field is correct
            tweet['tweet_id'] = tweet.get('tweet_id') 
            
            tweet_text = tweet.get('text', '')
            sentiment_result = self.GenerateSentiment(tweet_text)
            tweet['sentiment'] = sentiment_result.get('sentiment', 'UNKNOWN')

        except Exception as e:
            print(f"[ERROR] Failed to format Twitter data: {e}")
            return None

        return tweet

        
    def FormatIgData(self, post, refresh_id):
        """
        Formats Instagram data for MongoDB ingestion.
        Assumes user stats (follower/following count) have been merged into post['user'].
        """
        try:
            if not isinstance(post, dict) or not post.get('code'):
                print(f"[ERROR FormatIg] Invalid or empty post data received. Code: {post.get('code')}")
                return None
            
            formatted_data = {}
            metrics_data = post.get('metrics', {})

            caption_text = post.get('caption', {}).get('text', "")
            formatted_data['captions'] = caption_text
            sentiment_result = self.GenerateSentiment(caption_text)
            formatted_data['sentiment'] = sentiment_result.get('sentiment', 'UNKNOWN')
            
            # Instagram taken_at is already a timestamp (seconds since epoch)
            formatted_data['created_at'] = post.get('taken_at', post.get('taken_at_ts', 0)) 
            formatted_data['post_id'] = post.get('code') 

            
            user_data = post.get('user', {}) 
            if user_data:
                formatted_data['user_id'] = user_data.get('id')
                formatted_data['username'] = user_data.get('username')
                formatted_data['nickname'] = user_data.get('full_name')
                formatted_data['foto_profil'] = user_data.get('profile_pic_url')    
                # These fields are now assumed to be pre-merged in GetTweet
                formatted_data['followerCount'] = user_data.get('follower_count')
                formatted_data['followingCount'] = user_data.get('following_count')
            
            # Ensure metrics are safely cast to int or default to 0
            like_count = int(metrics_data.get('like_count') or 0)
            comment_count = int(metrics_data.get('comment_count') or 0)
            share_count = int(metrics_data.get('share_count') or 0)
            repost_count = int(metrics_data.get('repost_count') or 0)
            
            formatted_data['likes'] = like_count
            formatted_data['comments'] = comment_count 
            formatted_data['reposts'] = repost_count 
            formatted_data['share'] = share_count 
            # Engagement calculation includes views/shares/likes/comments
            formatted_data['engagement'] = (like_count + comment_count + repost_count + share_count) 
            formatted_data['refresh_id'] = refresh_id
            formatted_data['timestamp'] = int(time.time())
            
        except Exception as e:
            print(f"Critical error formatting Instagram data for code {post.get('code')}: {e}")
            traceback.print_exc() 
            return None
            
        return formatted_data
    
    
    def FormatTiktokData(self, post, refresh_id):
        """
        Formats TikTok data for MongoDB ingestion.
        Assumes user stats have been merged into post['author'].
        """
        try:
            formatted_data = {}

            if 'author' in post:
                author_data = post['author']
                # These fields are now assumed to be pre-merged in GetTweet
                formatted_data['user_id'] = author_data.get('id')
                formatted_data['username'] = author_data.get('unique_id')
                formatted_data['nickname'] = author_data.get('nickname')
                formatted_data['profil'] = author_data.get('avatar')
                formatted_data['followerCount'] = author_data.get('followerCount')
                formatted_data['followingCount'] = author_data.get('followingCount')
                formatted_data['videoCount'] = author_data.get('videoCount')
                        

            formatted_data['id_video'] = post.get('video_id')
            caption_text = post.get('title', '')
            formatted_data['capstion'] = caption_text
            sentiment_result = self.GenerateSentiment(caption_text)
            formatted_data['sentiment'] = sentiment_result.get('sentiment', 'UNKNOWN')

            # create_time is a Unix timestamp (seconds since epoch)
            formatted_data['created_at'] = post.get('create_time', 0) 
            
            # Ensure metrics are safely cast to int or default to 0
            digg = post.get('digg_count', 0)
            comments = post.get('comment_count', 0)
            shares = post.get('share_count', 0)
            views = post.get('play_count', 0)
            
            formatted_data['views'] = views 
            formatted_data['like'] = digg
            formatted_data['coments'] = comments
            formatted_data['share'] = shares 
            
            formatted_data['engagement'] = (views + digg + comments + shares) 
            
            formatted_data['refresh_id'] = refresh_id
            formatted_data['timestamp'] = int(time.time())
        
        except Exception as e:
            print(f"Error formatting TikTok data: {e}")
            return None
        return formatted_data


    def SaveTweets(self, posts, keyword=None, platform=None):
        """
        Saves posts to MongoDB using ReplaceOne with upsert=True 
        to handle duplicates gracefully (primary fix for Inserted: 0 issue).
        """
        if len(posts) == 0:
            return None
        
        # Define the unique ID field for each platform
        id_field_map = {
            'twitter': 'tweet_id',
            'instagram': 'post_id',
            'tiktok': 'id_video'
        }
        id_field = id_field_map.get(platform, 'post_id') # Default to 'post_id'
        
        bulk_update = []
        for post in posts:
            post_id_value = post.get(id_field)
            if not post_id_value:
                print(f"[MONGO WARNING] Skipping post due to missing unique ID field ({id_field})")
                continue
                
            doc = {
                **post,
                "topik": keyword,
                "id_project": self.args.id_project
            }
            if platform:
                doc["platform"] = platform
                
            # Use ReplaceOne to ensure existing duplicates are overwritten (upsert=True)
            bulk_update.append(ReplaceOne(
                {id_field: post_id_value},
                doc,
                upsert=True
            ))
            
        print(f"[MONGO] Writing {len(bulk_update)} posts to collection: {self.db_collection_name} using ReplaceOne.")
        try:
            res = self.db[self.db_collection_name].bulk_write(bulk_update)
            # Log inserted/upserted count
            inserted_count = res.upserted_count + res.matched_count 
            print(f"[MONGO] Upserted/Matched Count: {inserted_count}")
            return res
        except Exception as e:
            print(f"[MONGO ERROR] Failed to perform bulk write: {e}")
            return None

    # --- API Wrappers (Kept as is, assuming they are correct) ---

    def RapidAPITweet(self, keyword, cursor=None, rapidapi_key=None):
        url = "https://twitter-api45.p.rapidapi.com/search.php"
        querystring = {
            "query":"{} lang:id -filter:retweets".format(keyword),
            "search_type":"Latest",
        }
        if cursor:
            querystring['cursor'] = cursor
        headers = {
            "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY") if not rapidapi_key else rapidapi_key,
            "X-RapidAPI-Host": "twitter-api45.p.rapidapi.com"
        }
        try:
            response = requests.get(url, headers=headers, params=querystring, timeout=15)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"Twitter API Error: {response.status_code}")
                return None
        except Exception as e:
            print(f"Twitter Connection Error: {e}")
            return None

    def RapidAPIIg(self, keyword, cursor=None, rapidapi_key=None):
        url = "https://instagram-social-api.p.rapidapi.com/v1/search_posts"
        querystring = {
            "search_query": "{}".format(keyword)
        }
        if cursor:
            querystring['pagination_token'] = cursor
        headers = {
            "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY") if not rapidapi_key else rapidapi_key,
            "X-RapidAPI-Host": "instagram-social-api.p.rapidapi.com"
        }
        try:
            response = requests.get(url, headers=headers, params=querystring, timeout=25)
            if response.status_code == 200:
                data = response.json()
                if 'data' in data and 'items' in data['data']:
                    posts = data['data']['items']
                    posts_sorted = sorted(
                        posts, 
                        key=lambda post: post.get('taken_at_ts', 0), 
                        reverse=True
                    )
                    data['data']['items'] = posts_sorted
                return data
            else:
                print(f"Instagram Search API Error: {response.status_code}")
                return None
        except Exception as e:
            print(f"Instagram Search Connection Error: {e}")
            return None
        
    def RapidAPIIgPostInfo(self, shortcode, rapidapi_key=None):
        url = "https://instagram-social-api.p.rapidapi.com/v1/post_info"
        querystring = {
            "code_or_id_or_url": shortcode,
        }      
        headers = {
            "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY") if not rapidapi_key else rapidapi_key,
            "X-RapidAPI-Host": "instagram-social-api.p.rapidapi.com"
        }
        try:
            response = requests.get(url, headers=headers, params=querystring, timeout=25)
            if response.status_code == 200:
                json_data = response.json()
                if 'data' not in json_data:
                    print(f"[ERROR Post Info] No 'data' key in response for {shortcode}. Full response keys: {json_data.keys()}")
                    return {}
                detail_post = json_data.get('data', {}) 
                if not detail_post or 'code' not in detail_post:
                    # This often means the post is private or deleted
                    print(f"[WARN Post Info] Data detail kosong/invalid for {shortcode}. Status: {response.status_code}. Likely private/deleted.")
                    return {}
                return detail_post
            elif response.status_code == 429:
                print("Instagram Post Info API Error: 429 (Rate Limit Exceeded). Skipping current shortcode and waiting 2s.")
                time.sleep(2)
                return None
            else:
                print(f"Instagram Post Info API Error: {response.status_code} for shortcode: {shortcode}.")
                return None
        except Exception as e:
            print(f"Instagram Post Info Connection Error for {shortcode}: {e}")
            return None
        

    def RapidAPIIgUserInfo(self, user_id, rapidapi_key=None):
        url = "https://instagram-social-api.p.rapidapi.com/v1/info"
        querystring = {
            "username_or_id_or_url": str(user_id)
        } 
        headers = {
            "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY") if not rapidapi_key else rapidapi_key,
            "X-RapidAPI-Host": "instagram-social-api.p.rapidapi.com"
        }
        
        try:
            # Menggunakan loop sederhana untuk mencoba 2 kali jika status code 404/429
            for attempt in range(1, 3):
                response = requests.get(url, headers=headers, params=querystring, timeout=25)
                status = response.status_code

                if status == 200:
                    json_data = response.json()
                    if 'data' in json_data:
                        return json_data['data'] 
                    else:
                        print(f"[IG INFO] API response valid but missing 'data' key (Attempt {attempt}): {json_data.keys()}")
                        return None
                
                # Handling 404/429 (Not Found / Rate Limit)
                elif status == 404:
                    print(f"[IG INFO] Error 404 (Not Found) for user {user_id}. Attempt {attempt}. Response: {response.text[:50]}...")
                    if attempt < 2:
                        print("    -> Waiting 3s before retry.")
                        time.sleep(3) # Wait longer on 404
                    else:
                        return None
                elif status == 429:
                    print(f"[IG INFO] Error 429 (Rate Limit) for user {user_id}. Attempt {attempt}. Response: {response.text[:50]}...")
                    if attempt < 2:
                        print("    -> Waiting 5s before retry (Rate Limit).")
                        time.sleep(5) # Wait even longer on 429
                    else:
                        return None
                else:
                    print(f"Instagram User Info API Error: {status} (Attempt {attempt})")
                    return None
            
            return None # Failed after all attempts
            
        except Exception as e:
            print(f"Instagram User Info Connection Error for user {user_id}: {e}")
            return None
        

    def RapidAPITiktok(self, keyword, cursor=None, rapidapi_key=None):
        url = "https://tiktok-scraper7.p.rapidapi.com/feed/search" 
        querystring = {
            "keywords": "{} lang:id -filter:retweets".format(keyword), 
            "region": "id",
            "sort_type": "3"
        }
        if cursor is not None:
            querystring['cursor'] = str(cursor) 
        headers = {
            "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY") if not rapidapi_key else rapidapi_key,
            "X-RapidAPI-Host": "tiktok-scraper7.p.rapidapi.com" 
        }
        try:
            response = requests.get(url, headers=headers, params=querystring, timeout=15)
            if response.status_code == 200:
                return response.json()
            else:
                print(f"TikTok API Error: {response.status_code}")
                try: 
                    print(f"TikTok Error Response: {response.json()}")
                except: 
                    print(f"TikTok Error Text: {response.text}")
                return None
        except Exception as e:
            print(f"TikTok Connection Error: {e}")
            return None
        
    def RapidAPITiktokUserInfo(self, user_id, rapidapi_key=None):
        url = "https://tiktok-scraper7.p.rapidapi.com/user/info"
        querystring = {"user_id": str(user_id)}

        headers = {
            "X-RapidAPI-Key": os.getenv("RAPIDAPI_KEY") if not rapidapi_key else rapidapi_key,
            "X-RapidAPI-Host": "tiktok-scraper7.p.rapidapi.com"
        }
        
        try:
            response = requests.get(url, headers=headers, params=querystring, timeout=15)
            if response.status_code == 200:
                json_data = response.json()
                if json_data.get('code') == 0 and 'data' in json_data and 'stats' in json_data['data']:
                    return json_data['data']['stats']
                else:
                    print(f"TikTok User Info API returned status code 0 but no stats/data key: {json_data.get('msg')}")
                    return None
            else:
                print(f"TikTok User Info API Error: {response.status_code}")
                return None
        except Exception as e:
            print(f"TikTok User Info Connection Error for user {user_id}: {e}")
            return None
        
    def GetTweet(self, keyword, refresh_id, api_function, format_function, platform):
        """
        Main crawling logic, fetching posts page by page and handling 
        time window checks and platform-specific API calls.
        """
        
        max_page = int(self.args.page) if self.args.page else 8
        results = {"count": 0, "users_count": 0}
        
        v_minimum_id, v_minimum_timestamp = getTimeWindow(refresh_id, self.args.id_project)
        print(f"Minimum ID: {v_minimum_id} - Minimum Timestamp: {v_minimum_timestamp}")

        id_field_map = {
            'twitter': 'tweet_id',
            'instagram': 'post_id',
            'tiktok': 'id_video'
        }
        id_field = id_field_map.get(platform)
        
        v_cursor = None
        minimum_id_found = False
        time_window_exceeded = False 
        req_count = 0
        page = 1
        has_more = True 

        while not minimum_id_found and not time_window_exceeded and page <= max_page and has_more:
            
            posts_to_save = [] 
            
            print(f"[{platform.upper()}][Page {page}/{max_page}] Requesting API with cursor: {v_cursor}")

            req_result = api_function(keyword, cursor = v_cursor, rapidapi_key = self.args.api_key)
            req_count += 1
            
            if req_result:
                try:
                    v_post_items = []
                    v_next_cursor = None
                    
                    if platform == 'twitter':
                        v_post_items = req_result.get('timeline', [])
                        v_next_cursor = req_result.get('next_cursor')
                    elif platform == 'instagram':
                        v_post_items = req_result.get('data', {}).get('items', [])
                        v_next_cursor = req_result.get('pagination_token')
                    elif platform == 'tiktok':
                        v_post_items = req_result.get('data', {}).get('videos', [])
                        # Determine next cursor for TikTok
                        v_next_cursor_raw = req_result.get('data', {}).get('cursor')
                        v_next_cursor = v_next_cursor_raw
                    
                    print(f"[{platform.upper()}] Found {len(v_post_items)} items.")

                    for v_post_one_raw in v_post_items:
                        post = v_post_one_raw
                        
                        if platform == 'instagram':
                            shortcode = post.get('code') 
                            if not shortcode: continue
                            
                            # 1. Fetch Post Details (essential for full metrics/caption)
                            detail_post = self.RapidAPIIgPostInfo(shortcode, self.args.api_key)
                            req_count += 1 
                            
                            if not detail_post or not isinstance(detail_post, dict) or len(detail_post) < 5: 
                                print(f"[IG ENRICH] Post details unavailable for {shortcode}. Skipping.")
                                continue
                            post = detail_post # Use the detailed post object
                            
                            # 2. Fetch User Info (for follower/following count)
                            user_identifier = post.get('user', {}).get('username') # <-- Menggunakan 'username'
                            if user_identifier:
                                user_stats = self.RapidAPIIgUserInfo(user_identifier, self.args.api_key)
                                # Request count dan sleep sudah ditangani di RapidAPIIgUserInfo
                                req_count += 1 
                                
                                # Merge stats into the post['user'] object for use in FormatIgData
                                if user_stats and 'user' in post:
                                    post['user']['follower_count'] = user_stats.get('follower_count')
                                    post['user']['following_count'] = user_stats.get('following_count')
                                    print(f"[IG ENRICH] User {user_identifier} stats merged. Followers: {user_stats.get('follower_count')}")
                                else:
                                    # Jika user_stats is None (karena 404/429/error)
                                    print(f"[IG ENRICH] Failed/Skipped fetching user stats for {user_identifier}. Keeping null.")
                            
                        elif platform == 'tiktok':
                            # 1. Fetch User Info (for follower/following count)
                            user_id = post.get('author', {}).get('id')
                            if user_id:
                                user_stats = self.RapidAPITiktokUserInfo(user_id, self.args.api_key)
                                req_count += 1 
                                time.sleep(1) # Delay untuk TikTok User Info
                                
                                # Merge stats into the post['author'] object for use in FormatTiktokData
                                if user_stats and 'author' in post:
                                    post['author']['followerCount'] = user_stats.get('followerCount')
                                    post['author']['followingCount'] = user_stats.get('followingCount')
                                    post['author']['videoCount'] = user_stats.get('videoCount')
                                    print(f"[TT ENRICH] User {user_id} stats merged. Followers: {user_stats.get('followerCount')}")
                                else:
                                    print(f"[TT ENRICH] Failed/Skipped fetching user stats for {user_id}. Keeping null.")
                        # --- END ENRICHMENT ---

                        # 3. Format and Process Sentiment
                        post = format_function(post, refresh_id) 
                        if post is None: continue

                        # 4. Check Time Window
                        post_created_at = post.get('created_at', float('inf'))
                        if post_created_at < v_minimum_timestamp:
                            print(f"[{platform.upper()}] Time window exceeded ({post_created_at} < {v_minimum_timestamp}) -- break!")
                            time_window_exceeded = True
                            posts_to_save.append(post) # Save the last post before breaking
                            break
                        
                        # 5. Check Minimum ID
                        post_id_value = post.get(id_field)
                        if v_minimum_id is not None and post_id_value is not None and str(post_id_value) == str(v_minimum_id):
                            print(f"[{platform.upper()}] Minimum ID found ({v_minimum_id}) -- break!")
                            minimum_id_found = True
                            posts_to_save.append(post) # Save the post that matches the minimum ID
                            break
                            
                        posts_to_save.append(post)
                        
                    # 6. Save Batch
                    if len(posts_to_save) > 0:
                        print(f"[DEBUG] Saving {len(posts_to_save)} posts for keyword: {keyword}")
                        ins_res = self.SaveTweets(posts_to_save, keyword = keyword, platform=platform) 
                        if ins_res:
                            results['count'] += len(posts_to_save)
                            # Note: users_count calculation is too complex to fix easily in this file, keeping it at 0
                    
                    if minimum_id_found or time_window_exceeded:
                        break

                    if not v_post_items:
                        print(f"[{platform.upper()}] No posts found in response -- break!")
                        has_more = False 
                        break

                    v_cursor = v_next_cursor
                    # Explicitly check for end of pagination markers
                    if v_cursor is None or v_cursor == 0 or v_cursor == "":
                        has_more = False
                        
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(f"Error processing posts: {exc_type}, {fname}, line {exc_tb.tb_lineno} - {e}")
                    traceback.print_exc()
                    break
                    
            else:
                print(f"[{platform}] Error with RapidAPI, breaking.")
                time.sleep(1)
                break
                
            page += 1
            if page <= max_page:
                 time.sleep(1) 
        if page > max_page:
            print(f"Max page ({max_page}) reached -- stopping crawl.")

        return results, req_count
    

    def StartCrawl(self, refresh_id=None):
        if not refresh_id:
            refresh_id = int(time.time())

        target_sources = ['twitter', 'instagram', 'tiktok']
        
        total_req_count = 0
        self.db_collection_name = 'rapidapi_alexander'
        
        print("-" * 50)
        print(f"Target Sources: {target_sources}. Koleksi Target: **{self.db_collection_name}**.")
        print("-" * 50)
        
        for platform in target_sources:
            if platform == 'twitter':
                self.api_function = self.RapidAPITweet
                self.format_data_function = self.FormatTwitterData
            elif platform == 'instagram':
                self.api_function = self.RapidAPIIg
                self.format_data_function = self.FormatIgData
            elif platform == 'tiktok':
                self.api_function = self.RapidAPITiktok
                self.format_data_function = self.FormatTiktokData
            else:
                print(f"Peringatan: Platform '{platform}' tidak didukung. Melewatkan.")
                continue
            
            print(f"Mulai Crawling Platform: **{platform.upper()}**.")
                                        
            if self.args.keywords:
                arr_keywords = self.args.keywords.split(",")
                keywords = {"keywords": arr_keywords}
            else:
                keywords = GetKeywords(self.args.id_project)
                
            keywords['keywords'] = [k.strip() for k in keywords.get('keywords', [])]
            filtered_keywords = []
            for k in keywords['keywords']:
                if k.startswith('"') and k.endswith('"'):
                    filtered_keywords.append(k)
                    
            keywords['keywords'] = filtered_keywords
            total_keywords = len(keywords['keywords'])
            print(f"[KEYWORDS] Setelah filter, hanya {total_keywords} keyword bertanda kutip yang akan diproses.")
            
            if total_keywords == 0:
                print(f"Tidak ada keyword bertanda kutip ditemukan untuk platform {platform}. Melewatkan.")
                continue


            for index, keyword in enumerate(keywords['keywords']):
                print(f"[{platform.upper()}][{index+1}/{total_keywords}] Crawling Keyword: **{keyword}**")
                
                v_keyword_result, req_count = self.GetTweet(
                    keyword, 
                    refresh_id, 
                    self.api_function, 
                    self.format_data_function,
                    platform=platform 
                )
                total_req_count += req_count
                print(f"Result for {keyword}: {v_keyword_result}")
                print("-" * 50)
                
            addRequestCount(refresh_id, total_req_count, self.args.id_project)
            print(f"Selesai! Total Requests API: {total_req_count}")


    def __exit__(self, exc_type, exc_value, exc_traceback):
        print("----------- Crawler Run %s seconds -----------" %(time.time() - self.StartTime))