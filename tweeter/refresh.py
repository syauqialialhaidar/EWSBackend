import pandas as pd
import time
import datetime
from modules.mongo import appendStatusToWatchList, getActionFrom3Status, getWatchList, addToWatchList, addRefreshData, setWatchListStatus, getActiveWatchList, queryRefreshAggregate, addReport, getThreshold, getDataWithTimestamp
from tweeter.scrapper import GetKeywords 

def getPostId(tweet):
    if 'tweet_id' in tweet:
        return tweet['tweet_id'], 'twitter'
    elif 'id_video' in tweet:
        return tweet['id_video'], 'tiktok'
    elif 'post_id' in tweet:
        return tweet['post_id'], 'instagram'
    return None, None

def formTweetUrl(post_id, platform=None):
    if platform == 'twitter':
        return f"https://twitter.com/user/status/{post_id}"
    elif platform == 'instagram':
        return f"https://www.instagram.com/p/{post_id}/"
    elif platform == 'tiktok':
        return f"https://www.tiktok.com/@user/video/{post_id}" 
    else:
        return f"https://twitter.com/user/status/{post_id}" 


def getIdfromTweetUrl(url):
    id = url.split('/')
    return id[5]


def statusToNumber(status):
    if status == "Normal":
        return 0
    if status == "Early":
        return 1
    if status == "Emerging":
        return 2
    if status == "Current":
        return 3
    if status == "Crisis":
        return 4
    return -1


def refresh(timestamp, id_project):
    tweets = getDataWithTimestamp(timestamp, id_project)
    watchList = list(getActiveWatchList(id_project))
    unwatchList = watchList.copy()
    added = []
    
    for tweet in tweets:
        post_id, platform = getPostId(tweet)
        sentiments = tweet.get("sentiment")
        
        if post_id is None:
            print(f"[WARN] Skipping post with unknown ID format.")
            continue

        timestampPublikasi = tweet["created_at"]
        match = [x for x in watchList if x.get("tweet_id") == post_id or x.get("post_id") == post_id or x.get("id_video") == post_id]
        
        if len(match) == 0:
            addToWatchList(post_id, tweet["topik"], timestampPublikasi, id_project, platform=platform, sentiment=sentiments)
            watchList.append({"tweet_id": post_id, "is_active": True})
            print("Added: " + post_id)
            addRefreshData(tweet, timestamp, id_project)
            print("Refreshed: " + post_id)
            added.append(post_id)
        else:
            item_match = match[0]
            current_id = item_match.get("tweet_id") or item_match.get("post_id") or item_match.get("id_video")
            
            if item_match in unwatchList:
                unwatchList.remove(item_match)

            if item_match["is_active"] == False:
                continue
            else:
                if current_id not in added:
                    adder = 1
                    added.append(current_id)
                else:
                    adder = 0

                addToWatchList(current_id, tweet["topik"], timestampPublikasi, id_project, adder, platform=platform)
                addRefreshData(tweet, timestamp, id_project)
                
                print("Refreshed: " + current_id)
            
            added.append(current_id)
            
    for item in unwatchList:
        item_id = item.get("tweet_id") or item.get("post_id") or item.get("id_video")
        if item_id:
            print(item)
            setWatchListStatus(item_id, False, id_project)
            print("Expired: " + item_id)


def timeDiffToString(unixDiff):
    if unixDiff == None:
        return None
    if unixDiff < 60:
        return str(unixDiff) + " Detik"
    if unixDiff < 3600:
        return str(unixDiff // 60) + " Menit " + str(unixDiff % 60) + " Detik"
    if unixDiff < 86400:
        return str(unixDiff // 3600) + " Jam " + str((unixDiff % 3600) // 60) + " Menit"
    return str(unixDiff // 86400) + "Hari"


def EWSLogic(refresh_id, id_project=None):
    watchListWithTimestamps = list(getActiveWatchList(id_project))
    
    watchList = []
    for item in watchListWithTimestamps:
        if 'tweet_id' in item:
            watchList.append(item['tweet_id'])
        if 'post_id' in item:
            watchList.append(item['post_id'])
        if 'id_video' in item:
            watchList.append(item['id_video'])

    aggregate = [
        {
            '$match': {
                '$or': [
                    {'tweet_id': {'$in': watchList}},
                    {'post_id': {'$in': watchList}},
                    {'id_video': {'$in': watchList}},
                ]
            }
        },
        {
            '$sort': {'refresh_id': 1}
        },
        {
            '$unwind': {
                'path': '$engagement',
                'preserveNullAndEmptyArrays': True
            }
        },
        {
            '$group': {
                '_id': {
                    '$ifNull': [
                        '$tweet_id',  
                        {              
                            '$ifNull': [
                                '$post_id',
                                '$id_video'
                            ]
                        }
                    ]
                },
                'engagements': {'$push': '$engagement'}
            }
        }
    ]

    result = queryRefreshAggregate(aggregate)
    threshold = getThreshold(id_project)
    print("Treshhold ===== ", threshold)
    if threshold is None or "threshold" not in threshold or not threshold["threshold"]:
        print(f"[ERROR] Threshold data not found or empty for project ID: {id_project}. Aborting EWS logic.")
        return

    urls = []
    deltas = []
    engagement = []
    status = []
    percents = []
    thress = []
    timePosted = []
    timeRefreshed = []
    timeDelta = []
    keywords_list = GetKeywords(id_project)
    keywords = keywords_list["keywords"]
    keywords = {x: {"totalEngagements": 0, "totalDelta": 0} for x in keywords}
    
    query = [
        {
            "$match": {
                "refresh_id": refresh_id,
                "id_project": id_project
            }
        }
    ]
    tweets = queryRefreshAggregate(query)
    tweets = list(tweets)
    
    for item in result:
        unique_post_id = item["_id"]
        
        watch_match = [x for x in watchListWithTimestamps if x.get("tweet_id") == unique_post_id or x.get("post_id") == unique_post_id or x.get("id_video") == unique_post_id]
        latest_tweet_doc = next((t for t in tweets if t.get('tweet_id') == unique_post_id or t.get('post_id') == unique_post_id or t.get('id_video') == unique_post_id), None)
        sentiment_value = latest_tweet_doc.get('sentiment', 'N/A') if latest_tweet_doc else 'N/A'
        platform = latest_tweet_doc.get('platform') if latest_tweet_doc else None
        urls.append(formTweetUrl(unique_post_id, platform)) 
        engagement.append(item["engagements"][-1])
        timeRefreshed.append(datetime.datetime.fromtimestamp(refresh_id))

        if len(watch_match) == 1:
            watch = watch_match[0]
            
            for key in watch["topik"]:
                if key not in keywords:
                    keywords[key] = {"totalEngagements": 0, "totalDelta": 0}
                    
                keywords[key]["totalEngagements"] += item["engagements"][-1]
                
                if len(item["engagements"]) > 1:
                    keywords[key]["totalDelta"] += item["engagements"][-1] - item["engagements"][-2]

            posted = watch["timestamp_publikasi"]
            timePosted.append(datetime.datetime.fromtimestamp(posted))
            timeDelta.append(timeDiffToString(refresh_id - posted))
        else:
            timePosted.append(None)
            timeDelta.append(None)


        if len(item["engagements"]) == 1:
            deltas.append(None)
            percents.append(None)
            thres = threshold["threshold"][0]
            thress.append(thres)
            t_status = "Normal"
            if item["engagements"][0] > thres["early"] and item["engagements"][0] < thres["emerging"]:
                t_status = "Early"
            elif item["engagements"][0] > thres["emerging"] and item["engagements"][0] < thres["current"]:
                t_status = "Emerging"
            elif item["engagements"][0] > thres["current"] and item["engagements"][0] < thres["crisis"]:
                t_status = "Current"
            elif item["engagements"][0] > thres["crisis"]:
                t_status = "Crisis"
            status.append(t_status)
            print(t_status, statusToNumber(t_status), id_project)
            appendStatusToWatchList(unique_post_id, statusToNumber(t_status), id_project)
            continue
            
        delta = item["engagements"][-1] - item["engagements"][-2]
        deltas.append(delta)

        prevThres = None
        before = item["engagements"][-2]

        for thres in reversed(threshold["threshold"]):
            if thres["before"] is None:
                break
            if before < thres["before"]:
                prevThres = thres
                continue
        thres = prevThres
        if thres is None:
            thres = threshold["threshold"][-1]

        percent = delta / (before + 1) * 100
        percents.append(percent)
        thress.append(thres)
        t_status = "Normal"

        if percent > thres["early"] and percent < thres["emerging"]:
            t_status = "Early"
        elif percent > thres["emerging"] and percent < thres["current"]:
            t_status = "Emerging"
        elif percent > thres["current"] and percent < thres["crisis"]:
            t_status = "Current"
        elif percent > thres["crisis"]:
            t_status = "Crisis"
        status.append(t_status)
        print(t_status, statusToNumber(t_status), id_project)

        appendStatusToWatchList(unique_post_id, statusToNumber(t_status), id_project)
        
    report = {
                "refresh_id": refresh_id,
                "id_project": id_project,
                "urls": urls,
                "deltas": deltas,
                "engagements": engagement,
                "status": status,
                "threshold": thress,
                "time_posted": timePosted,
                "time_refreshed": timeRefreshed,
                "time_delta": timeDelta,
                "percents": percents,
                "tweets": tweets,
                "keywords": keywords,
                }
    addReport(report)

def redetermineWatchList(id_project = None):
    watchList = list(getActiveWatchList(id_project))
    for watch in watchList:
        dt = datetime.datetime.fromtimestamp(watch["timestamp_publikasi"])
        watch_id = watch.get("tweet_id") or watch.get("post_id") or watch.get("id_video")

        if 20 <= dt.hour < 4:
            setWatchListStatus(watch_id, False, id_project)

        elif watch["refresh_count"] == 4:
            action = getActionFrom3Status(watch["status"][-3], watch["status"][-2], watch["status"][-1])
            if action is not None and action['a'] == 0:
                setWatchListStatus(watch_id, False, id_project)
        elif watch["refresh_count"] > 4:
            if watch["status"][-1] < watch["status"][-2]:
                setWatchListStatus(watch_id, False, id_project)