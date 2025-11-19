from datetime import datetime
from modules.mongo import setAllWatchListStatus
from tweeter.refresh import EWSLogic, redetermineWatchList, refresh
from tweeter.scrapper import ScriptFix
import argparse
from dotenv import load_dotenv
import time
import argparse


parser = argparse.ArgumentParser()


load_dotenv()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-p", "--page", help="Page of retweet to search")
    parser.add_argument("-k", "--keywords", help="Keyword to search")
    parser.add_argument("-i", "--id_project", help="ID Project")
    parser.add_argument("-a", "--api_key", help="API Key")
    parser.add_argument("-l", "--limit", help="Limit of tweet to search")
    args = parser.parse_args()

    timestamp = int(time.time())
    dt = datetime.fromtimestamp(timestamp)
    #jam 5 mulai, jam 8 selesai
    if dt.hour >= 21 and dt.hour <= 4:
        print(f"--------Jam : {dt.hour}. SKIPPING----------")
        exit(0)

    with ScriptFix(args) as crawler:
        crawler.StartCrawl(refresh_id=timestamp)
    refresh(timestamp, args.id_project)
    EWSLogic(timestamp, args.id_project)
    redetermineWatchList(args.id_project)
    if dt.hour >= 20:
        #dropAll
        setAllWatchListStatus(False, args.id_project)