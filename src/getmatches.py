##################################################################
#  AIM : Get All ICC Matches since a threshold
# basically from the schedules
# 
##################################################################

##################################################################
# imports
##################################################################
import time, random, requests, json, traceback, logging, os, pandas as pd, tqdm
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from threading import Semaphore
from bidmap import idmap
##################################################################



##################################################################
# Constants/config
##################################################################
MAX_WORKERS = 20
CONCURRENT_REQUESTS = 50
requsoph = Semaphore(CONCURRENT_REQUESTS)

OUTPUT_DIRECORY = "../data/raw/matches/"
os.makedirs(OUTPUT_DIRECORY, exist_ok=True)
LOGDIR = "../logs/getmatches/"
os.makedirs(LOGDIR, exist_ok=True)
SCHEDULE = "../data/raw/schedules/"
##################################################################



##################################################################
# CHECKS/Functions
##################################################################
def fourdigitpad(num,n):
    return str(num).zfill(n)

def already_exists(id):
    if not os.path.exists(OUTPUT_DIRECORY+f"{id}"):
        return False
    if "scorecard.json" in os.listdir(OUTPUT_DIRECORY + f"{id}"):
        return True
    else:
        return False


##################################################################



#################################################
# MAINS

def fetch(url):
    with requsoph:
        time.sleep(random.uniform(0.08,0.23))
        try:
                response = requests.get(url, timeout = 10)
                response.raise_for_status()
                x = response.json()
                if x.get("data"):
                    return x["data"]
                else:
                    return None
        except:
            logpath = os.path.join(LOGDIR, "getmatches.log")
            er = traceback.format_exc()
            # logging.basicConfig(filename=logpath, filemode="w", level=logging.DEBUG)
            # logging.basicConfig(filename=logpath, filemode="w", level=logging.DEBUG)
            return None

def downloadmatch(mid):
    id = idmap(mid)
    if already_exists(id):
        return False
    else:
        mdir = os.path.join(OUTPUT_DIRECORY, str(id))
        os.makedirs(mdir, exist_ok=True)
    urls = {
        "scorecard": f"https://assets-icc.sportz.io/cricket/v1/game/scorecard?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&game_id={mid}&lang=en",
        "bat1": f"https://assets-icc.sportz.io/cricket/v1/game/batsman-split?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&game_id={mid}&inning=1&lang=en",
        "bat2": f"https://assets-icc.sportz.io/cricket/v1/game/batsman-split?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&game_id={mid}&inning=2&lang=en",
        "bat3": f"https://assets-icc.sportz.io/cricket/v1/game/batsman-split?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&game_id={mid}&inning=3&lang=en",
        "bat4": f"https://assets-icc.sportz.io/cricket/v1/game/batsman-split?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&game_id={mid}&inning=4&lang=en",
        "comm1": f"https://assets-icc.sportz.io/cricket/v1/game/commentary?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&game_id={mid}&inning=1&lang=en&page_number=1&page_size=1000",
        "comm2": f"https://assets-icc.sportz.io/cricket/v1/game/commentary?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&game_id={mid}&inning=2&lang=en&page_number=1&page_size=1000",
        "comm3": f"https://assets-icc.sportz.io/cricket/v1/game/commentary?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&game_id={mid}&inning=3&lang=en&page_number=1&page_size=1000",
        "comm4": f"https://assets-icc.sportz.io/cricket/v1/game/commentary?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&game_id={mid}&inning=4&lang=en&page_number=1&page_size=1000"
    }
    results = {}
    for key, url in urls.items():
        data = fetch(url)
        # print(key, data)
        if data:
            results[key] = data
        else:
            prob = traceback.format_exc()
            with open(LOGDIR+"/getmatches.log", "a") as f:
                f.write(f"{mid}, {id}, {key}")
                f.write(prob+"\n")

    if "scorecard" in results:
        with open(mdir + "/scorecard.json", "w") as f:
            json.dump(results["scorecard"], f, indent=4)
    if "comm1" in results:
        os.makedirs(mdir + "/1", exist_ok=True)
        dir = mdir + "/1/commentary.json"
        with open(dir, "w") as f:
            json.dump(results["comm1"], f, indent=4)
    if "bat1" in results:
        os.makedirs(mdir + "/1", exist_ok=True)
        dir = mdir + "/1/batsplit.json"
        with open(dir, "w") as f:
            json.dump(results["bat1"], f, indent=4)
    if "comm2" in results:
        os.makedirs(mdir + "/2", exist_ok=True)
        dir = mdir + "/2/commentary.json"
        with open(dir, "w") as f:
            json.dump(results["comm2"], f, indent=4)
    if "bat2" in results:
        os.makedirs(mdir + "/2", exist_ok=True)
        dir = mdir + "/2/batsplit.json"
        with open(dir, "w") as f:
            json.dump(results["bat2"], f, indent=4)
    if "comm1" in results:
        os.makedirs(mdir + "/1", exist_ok=True)
        dir = mdir + "/1/commentary.json"
        with open(dir, "w") as f:
            json.dump(results["comm2"], f, indent=4)
    if "bat1" in results:
        os.makedirs(mdir + "/1", exist_ok=True)
        dir = mdir + "/1/batsplit.json"
        with open(dir, "w") as f:
            json.dump(results["bat2"], f, indent=4)
    if "comm3" in results:
        os.makedirs(mdir + "/3", exist_ok=True)
        dir = mdir + "/3/commentary.json"
        with open(dir, "w") as f:
            json.dump(results["comm3"], f, indent=4)
    if "bat3" in results:
        os.makedirs(mdir + "/3", exist_ok=True)
        dir = mdir + "/3/batsplit.json"
        with open(dir, "w") as f:
            json.dump(results["bat3"], f, indent=4)
    if "comm4" in results:
        os.makedirs(mdir + "/4", exist_ok=True)
        dir = mdir + "/4/commentary.json"
        with open(dir, "w") as f:
            json.dump(results["comm4"], f, indent=4)
    if "bat4" in results:
        os.makedirs(mdir + "/4", exist_ok=True)
        dir = mdir + "/4/batsplit.json"
        with open(dir, "w") as f:
            json.dump(results["bat4"], f, indent=4)

    return True
##################################################



##################################################
# Scheduleprocess
##################################################
def process(schedulefile):
    with open(schedulefile, "r") as f:
        data = json.load(f).get("data",{})
        matches = data.get("matches",[])
        return [int(m.get('match_id')) for m in matches if m.get('match_id')]
##################################################


##################################################
# Process
##################################################
if __name__ == "__main__":
    files = os.listdir(SCHEDULE)
    IDs = []
    for file in files:
        ids = process(SCHEDULE+file)
        if ids:
            IDs.extend(ids)
    # Done = []
    print(f"Found {len(IDs)} matches, why they play so much")
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(downloadmatch, id) for id in IDs}
        with tqdm.tqdm(total = len(IDs)) as pbar:
            for count, future in (enumerate(as_completed(futures))):
                try:
                    future.result()
                except Exception as e:
                    pass
                    # with open(LOGDIR+"/getmatches.log", "a") as f:
                    #     f.write(f"{count}, {e}, {traceback.format_exc()}")

                finally:
                    pbar.update(1)