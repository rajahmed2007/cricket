##################################################################
#  AIM : Get All ICC Match Schedules since a threshold
# for now the threshold is set to 2010, but it can be changed to any year
# And we dont care pre 2010, fuck
# 
##################################################################



#################################################
#  IMPORTS
#################################################
import os, time, json, requests, tqdm,logging , traceback, random, pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
#################################################


################################################
#  Configuration
################################################
HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"}
START_DATE = "20100101"
END_DATE = "20260225"
MAX_WORKERS = 10 # Dont want to overload the server, so we will use a max of 10 workers, basically i dont care abiut morality but do about ip block
OUTPUT_DIRECORY = "../data/raw/schedules/"
LOGDIR = "../logs/getschedules/"
################################################



################################################
# BASIC FUNCTIONS AND CHECKS
################################################
if not os.path.exists(OUTPUT_DIRECORY):
    os.makedirs(OUTPUT_DIRECORY)
if not os.path.exists(LOGDIR):
    os.makedirs(LOGDIR)
def fourdigitpad(num):
    return str(num).zfill(4)

def uniquepageno(pno):
    date = pd.to_datetime(START_DATE) + pd.Timedelta(days=(pno-1)*1000)
    return date.strftime("%Y%m%d") + str(fourdigitpad(pno))

def already_exists(uniqupageno):
    return os.path.exists(os.path.join(OUTPUT_DIRECORY, f"{uniqupageno}.json"))    

################################################
# GLOBAL
################################################

def gettotalpages():
    url = f"https://assets-icc.sportz.io/cricket/v1/schedule?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&from_date={START_DATE}&is_deleted=false&is_live=false&is_recent=true&is_upcoming=false&lang=en&page_number=1&page_size=1000&to_date={END_DATE}&timezone=0530" 
    try:
        response = requests.get(url, headers=HEADERS)
        if response.status_code == 200:
            data = response.json()
            total_pages = data.get("meta", {}).get("count", 0)
            return total_pages//1000 + 1 # since page size is 1000
        else:
            logfile = os.path.join(LOGDIR, "getschedules.log")
            rawm = traceback.format_exc()
            logging.info(f"Failed to get total pages. Status code: {response.status_code}. Raw error: {rawm}")
            return 0
    except Exception as e:
        logfile = os.path.join(LOGDIR, "getschedules.log")
        rawm = traceback.format_exc()
        logging.info(f"Exception while getting total pages. Raw error: {rawm}")
        return 0
    
# print(gettotalpages())






################################################
# Main Function to get schedules
################################################
def fetchpage(pageno):
    url = f"https://assets-icc.sportz.io/cricket/v1/schedule?client_id=tPZJbRgIub3Vua93%2FDWtyQ%3D%3D&feed_format=json&from_date={START_DATE}&is_deleted=false&is_live=false&is_recent=true&is_upcoming=false&lang=en&page_number={pageno}&page_size=1000&to_date={END_DATE}&timezone=0530" 
    unqiueno = uniquepageno(pageno)
    if already_exists(unqiueno):
        logfile = os.path.join(LOGDIR, "getschedules.log")
        rawm = traceback.format_exc()
        logging.basicConfig(filename=logfile, filemode="w", level=logging.DEBUG)
        return False

    # Jitter
    time.sleep(random.uniform(0.44, 1.3))
    
    try:
        response = requests.get(url, headers=HEADERS)
        # print(response.status_code)
        if response.status_code == 200:
            with open(os.path.join(OUTPUT_DIRECORY, f"{unqiueno}.json"), "w") as f:
                json.dump(response.json(), f)
            return True
        else:
            logfile = os.path.join(LOGDIR, "getschedules.log")
            rawm = traceback.format_exc()
            logging.info(f"Failed to fetch page {pageno}. Status code: {response.status_code}. Raw error: {rawm}")
    except Exception as e:
        logfile = os.path.join(LOGDIR, "getschedules.log")
        rawm = traceback.format_exc()
        logging.info(f"Exception while fetching page {pageno}. Raw error: {rawm}")
        
#################################################
#   The work
################################################
# no need of workers for now as its few pages (literally 12-15)
totalpages = gettotalpages()
for pageno in tqdm.tqdm(range(1, totalpages+1)):
    fetchpage(pageno) 
################################################
