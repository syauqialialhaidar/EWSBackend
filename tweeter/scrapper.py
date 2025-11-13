from datetime import date, datetime, timedelta
import pytz
import time
import os, sys
from dotenv import load_dotenv
from pathlib import Path
import requests
from pymongo import InsertOne
from dateutil.parser import parse
from modules.mongo import addRequestCount, db, client, getMinimumActiveTimestamp 
import json
import traceback

load_dotenv()


def calculateEngagement(post):
    try:
        retweet = post.get('retweets', 0)
        favorite = post.get('favorites', 0)
        replies = post.get('replies', 0)
        return (retweet + favorite + replies)
    except:
        return 0

def GetKeywords(id_project = None):
    succ = False
    v_api = "https://api.kurasi.media/v2/crawler/get-keyword-all/medsos"
    v_params = {
        "source": "twitter, instagram, tiktok"
    }
    if id_project:
        v_params['id_project'] = id_project
    while not succ:
    # get keyword list
        try:
            api = requests.get(v_api, params=v_params)

            keyword_list = api.json()
            succ = True
            return keyword_list
        except:
            print("FAILED. TRY TO REQUEST KEYWORDS AGAIN...")
            

def getTimeWindow(start_timestamp, id_project):
    start_datetime = datetime.fromtimestamp(start_timestamp)
    if start_datetime.hour == 5:
        yesterday = start_datetime - timedelta(days=1)
        yesterdayAt9 = yesterday.replace(hour=20, minute=0, second=0)
        unix_time = int(time.mktime(yesterdayAt9.timetuple()))
        return None, unix_time

    v_minimum = getMinimumActiveTimestamp(id_project)

    if v_minimum:
        v_minimum_timestamp = v_minimum['timestamp_publikasi']
        v_minimum_id = v_minimum['tweet_id']
    else:
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
        print(" Glavier Social Media Search V2")
        print("--------------------------------------------------------")
        print("Started Time:", self.datetime_ist)
        print("Args: {}".format(self.args.__dict__))
        self.db = db
        return self

    def GenerateSentiment(self, text_content):
        if not text_content:
            return {'sentiment': 'NEUTRAL', 'confidence': 1.0, 'source': 'empty_text'}
            
        url = "https://generate-sentiment-naive-baiyes.onlinemonitoring.id/sentiment/generate"
        headers = {
            "Content-Type": "application/json"
        }
        payload = json.dumps({"data": [text_content]})
        
        try:
            response = requests.post(url, headers=headers, data=payload) 
            
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
                print(f"[SENTIMENT API] Error {response.status_code}: {error_detail}")
                return {"sentiment": "ERROR", "confidence": 0.0, "api_status": response.status_code}
        except requests.exceptions.ReadTimeout as e:
            print(f"[SENTIMENT API] Connection Error: Read timed out. (Timeout set to 20s)")
            return {"sentiment": "TIMEOUT", "confidence": 0.0, "api_error": str(e)}
        except Exception as e:
            print(f"[SENTIMENT API] Connection Error: {e}")
            return {"sentiment": "ERROR", "confidence": 0.0, "api_error": str(e)}

    def ConvertDate(self, date):
        dt = parse(date)
        return int(dt.timestamp())

    def FormatTwitterData(self, tweet, refresh_id):
        try:
            v_date = tweet['created_at']
            tweet['created_at'] = self.ConvertDate(v_date)
            tweet['refresh_id'] = refresh_id
            tweet['engagement'] = calculateEngagement(tweet)
            tweet['timestamp'] = int(time.time())
            tweet['tweet_id'] = tweet['tweet_id']
            tweet_text = tweet.get('text', '')
            sentiment_result = self.GenerateSentiment(tweet_text)
            tweet['sentiment'] = sentiment_result.get('sentiment', 'UNKNOWN')

        except Exception as e:
            print(f"[ERROR] Failed to format Twitter data: {e}")
            return None

        return tweet

        
    def FormatIgData(self, post, refresh_id):
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
        
            
            formatted_data['created_at'] = post.get('taken_at', 0)
            formatted_data['post_id'] = post.get('code') 

            
            user_data = post.get('user', {}) 
            if user_data:
                formatted_data['user_id'] = user_data.get('id')
                formatted_data['username'] = user_data.get('username')
                formatted_data['nickname'] = user_data.get('full_name')
                formatted_data['foto_profil'] = user_data.get('profile_pic_url') 
            
            like_count = int(metrics_data.get('like_count') or 0)
            comment_count = int(metrics_data.get('comment_count') or 0)
            share_count = int(metrics_data.get('share_count') or 0)
            repost_count = int(metrics_data.get('repost_count') or 0)
            
            formatted_data['likes'] = like_count
            formatted_data['comments'] = comment_count 
            formatted_data['reposts'] = repost_count 
            formatted_data['share'] = share_count 
            formatted_data['engagement'] = (like_count + comment_count + repost_count + share_count) 
            formatted_data['refresh_id'] = refresh_id
            formatted_data['timestamp'] = int(time.time())
            
        except Exception as e:
            print(f"Critical error formatting Instagram data for code {post.get('code')}: {e}")
            traceback.print_exc() 
            return None
            
        return formatted_data
    
    
    def FormatTiktokData(self, post, refresh_id):
        try:
            formatted_data = {}

            if 'author' in post:
                author_data = post['author']
                user_id = author_data.get('id')
                formatted_data['user_id'] = user_id
                formatted_data['username'] = author_data.get('unique_id')
                formatted_data['nickname'] = author_data.get('nickname')
                formatted_data['profil'] = author_data.get('avatar')
                
                # --- [PENAMBAHAN BARU] ---
                if user_id:
                    print(f"[TIKTOK INFO] Mengambil info pengguna untuk ID: {user_id}")
                    user_stats = self.RapidAPITiktokUserInfo(user_id, self.args.api_key)
                    
                    if user_stats:
                        formatted_data['followerCount'] = user_stats.get('followerCount')
                        formatted_data['followingCount'] = user_stats.get('followingCount')
                        formatted_data['videoCount'] = user_stats.get('videoCount')
                    else:
                        print(f"[TIKTOK INFO] Gagal mendapatkan statistik pengguna untuk ID: {user_id}")
                        formatted_data['followerCount'] = None
                        formatted_data['followingCount'] = None
                        formatted_data['videoCount'] = None
                        
                # --- [AKHIR PENAMBAHAN BARU] ---

            formatted_data['id_video'] = post['video_id']
            caption_text = post.get('title', '')
            formatted_data['capstion'] = caption_text
            sentiment_result = self.GenerateSentiment(caption_text)
            formatted_data['sentiment'] = sentiment_result.get('sentiment', 'UNKNOWN')

            formatted_data['created_at'] = post['create_time'] 
            
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
        if len(posts) == 0:
            return None
        
        bulk_update = []
        for post in posts:
            doc = {
                **post,
                "topik": keyword,
                "id_project": self.args.id_project
            }
            if platform:
                doc["platform"] = platform 
                
            bulk_update.append(InsertOne(doc))
            
        print(f"[MONGO] Writing {len(bulk_update)} posts to collection: {self.db_collection_name}")
        try:
            res = self.db[self.db_collection_name].bulk_write(bulk_update)
            return res
        except Exception as e:
            print(f"[MONGO ERROR] Failed to perform bulk write: {e}")
            return None

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
            response = requests.get(url, headers=headers, params=querystring, timeout=15)
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
            response = requests.get(url, headers=headers, params=querystring, timeout=15)
            if response.status_code == 200:
                json_data = response.json()
                if 'data' not in json_data:
                    print(f"[ERROR Post Info] No 'data' key in response for {shortcode}. Full response keys: {json_data.keys()}")
                    return {}
                detail_post = json_data.get('data', {}) 
                if not detail_post or 'code' not in detail_post:
                    print(f"[WARN Post Info] Data detail kosong atau tidak valid for {shortcode}. Post likely private/deleted.")
                    return {}
                return detail_post
            elif response.status_code == 429:
                print("Instagram Post Info API Error: 429 (Rate Limit Exceeded). Skipping current shortcode and waiting 5s.")
                time.sleep(2)
                return None
            else:
                print(f"Instagram Post Info API Error: {response.status_code} for shortcode: {shortcode}. "
                    f"Text: {response.text[:100]}...")
                return None
        except Exception as e:
            print(f"Instagram Post Info Connection Error for {shortcode}: {e}")
            return None

    def RapidAPITiktok(self, keyword, cursor=None, rapidapi_key=None):
        url = "https://tiktok-scraper7.p.rapidapi.com/feed/search" 
        querystring = {
            "keywords": "{} lang:id -filter:retweets".format(keyword), 
            "region": "id",
            "sort_type": "1"
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
                        v_next_cursor_raw = req_result.get('data', {}).get('cursor')
                        if v_next_cursor_raw:
                            v_next_cursor = int(v_next_cursor_raw)
                        elif len(v_post_items) > 0:
                            v_next_cursor = page * 10
                        
                    print(f"[{platform.upper()}] Found {len(v_post_items)} items.")

                    for v_post_one_raw in v_post_items:
                        post = v_post_one_raw
                        if platform == 'instagram':
                            shortcode = post.get('code') 
                            if not shortcode: continue
                            
                            detail_post = self.RapidAPIIgPostInfo(shortcode, self.args.api_key)
                            req_count += 1 
                            time.sleep(1) 
                            
                            if not detail_post or not isinstance(detail_post, dict) or len(detail_post) < 5: 
                                continue
                            post = detail_post
                        
                        elif platform == 'tiktok':
                            user_id = post.get('author', {}).get('id')
                            if user_id:
                                req_count += 1 
                                time.sleep(1) 


                        post = format_function(post, refresh_id) 
                        if post is None: continue

                        if post.get('created_at', float('inf')) < v_minimum_timestamp:
                            print(f"[{platform.upper()}] Time window exceeded ({post.get('created_at')} < {v_minimum_timestamp}) -- break!")
                            time_window_exceeded = True
                            break
                        
                        post_id_value = post.get(id_field)
                        if v_minimum_id is not None and post_id_value is not None and post_id_value == v_minimum_id:
                            print(f"[{platform.upper()}] Minimum ID found ({v_minimum_id}) -- break!")
                            minimum_id_found = True
                            posts_to_save.append(post)
                            break
                            
                        posts_to_save.append(post)
                        
                    if len(posts_to_save) > 0:
                        print(f"[DEBUG] Saving {len(posts_to_save)} posts for keyword: {keyword}")
                        ins_res = self.SaveTweets(posts_to_save, keyword = keyword, platform=platform) 
                        if ins_res:
                            inserted_count = ins_res.bulk_api_result.get('insertedCount', 0) if hasattr(ins_res, 'bulk_api_result') else len(posts_to_save)
                            print(f"[MONGO] Inserted: {inserted_count}")
                        
                        results['count'] += len(posts_to_save)
                        unique_users = set(post.get('user_id') or post.get('user', {}).get('id') for post in posts_to_save) 
                    if minimum_id_found or time_window_exceeded:
                        break

                    if not v_post_items:
                        print(f"[{platform.upper()}] No posts found in response -- break!")
                        has_more = False 
                        break

                    v_cursor = v_next_cursor
                    if v_cursor is None or v_cursor == 0:
                        has_more = False
                        
                except Exception as e:
                    exc_type, exc_obj, exc_tb = sys.exc_info()
                    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
                    print(f"Error processing posts: {exc_type}, {fname}, line {exc_tb.tb_lineno} - {e}")
                    break
                    
            else:
                print(f"[{platform}] Error with RapidAPI, breaking.")
                time.sleep(1)
                break
                
            page += 1
        if page > max_page:
            print(f"Max page ({max_page}) reached -- break!")

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
                
            limit = int(self.args.limit) if self.args.limit else 5
            keywords['keywords'] = [k.strip() for k in keywords['keywords']][:limit]

            total_keywords = len(keywords['keywords'])

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