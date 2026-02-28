##################################################################
#  AIM : Get All ICC Matchesto be in my own jsobnr
# basically from the schedules
#
##################################################################
import datetime

##################################################################
# IMPORYS
##################################################################
from bidmap import idmap
import json, os, traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
import time, tqdm
import numpy as np, pandas as pd
from datetime import date
# datetime.date.today() bakchdosi
###################################################################


###################################################################
# DRUNK VAR NAME
###################################################################
alrea = []
Leagues = []
serieses = []
tours = []
venues = []
noBbBd = []
###################################################################


###################################################################
# dirs
###################################################################
dis = "../database/"
os.makedirs(dis, exist_ok=True)
MATCHES = "../data/raw/matches"
OUTDIR = "../data/processed/cricdatacleanedandwithoutanyintrusivethoughtsandidespisecodingespeciallyfucklifefuckbigbang/"
os.makedirs(OUTDIR, exist_ok=True)
###################################################################


###################################################################
# Util functions
###################################################################
def remove_nones(d):
    if isinstance(d, dict):
        return {k: remove_nones(v) for k, v in d.items() if v is not None}
    elif isinstance(d, list):
        return [remove_nones(v) for v in d if v is not None]
    return d
def ndigitpad(num,n):
    return str(num).zfill(n)

def already_exist(matchid):
    if os.path.exists(f"{OUTDIR}/{matchid}.json"):
        return True
    else:
        return False
###################################################################


def parsematch(id, registry):
    basics = {}
    with open(f"{MATCHES}/{id}/scorecard.json", "r") as f:
        d = json.load(f)
        maindetail = d.get('Matchdetail', {})

        awards = maindetail.get("Awards", {})
        Awards = []
        if awards:
            for award in awards:
                award_id_raw = award.get("Award_Type")
                player_id_raw = award.get("Player_Id")
                # award_id = idmap(int(award_id_raw)) if award_id_raw is not None else None
                player_id = idmap(int(player_id_raw)) if player_id_raw else None
                team_id_raw = award.get("Team_Id")
                team_id = idmap(int(team_id_raw)) if team_id_raw else None
                Awards.append({
                    "award_type": award_id_raw,
                    "player": player_id,
                    "team": team_id
                })

                # registry[award_id] = award.get("Award_Type")

        thid_raw = maindetail.get("Team_Home")
        taid_raw = maindetail.get("Team_Away")
        thid = int(thid_raw) if thid_raw else None
        taid = int(taid_raw) if taid_raw else None

        t1id = idmap(thid) if thid else None
        t2id = idmap(taid) if taid else None

        match = maindetail.get("Match", {})
        lid_raw = match.get("League_Id")
        league = match.get("League")

        lid = None
        if lid_raw is not None:
            lid = idmap(int(lid_raw))
            if lid not in Leagues:
                Leagues.append({
                    "id": lid,
                    "name": league
                })
                alrea.append(lid)

        League = {
            "id": lid,
            "name": league,
            "group": match.get("Group")
        }

        match_id_raw = match.get("Id")
        id = idmap(int(match_id_raw)) if match_id_raw else None

        date = match.get("Date")
        type = match.get("Type")
        num = match.get("Number")

        daynight_raw = match.get("Daynight")
        daynight = None
        if daynight_raw is not None:
            daynight = False if daynight_raw == "no" else True

        Series_raw = maindetail.get("Series", {})
        seriesid = None
        tourid = None
        name = None
        tourname = None

        if Series_raw:
            sirid_raw = Series_raw.get("Id")
            name = Series_raw.get("Name")
            tourid_raw = Series_raw.get("Tour")

            if tourid_raw is not None:
                tourid = idmap(int(tourid_raw))
                tourname = Series_raw.get("Tour_Name")
                has_stand = False if Series_raw.get("Has_standings") == "false" else True

                if tourid not in alrea:
                    tours.append({
                        "id": tourid,
                        "name": tourname,
                        "Has_Standings": has_stand
                    })
                    alrea.append(tourid)

            if sirid_raw is not None:
                seriesid = idmap(int(sirid_raw))
                if seriesid not in alrea:
                    serieses.append({
                        "id": seriesid,
                        "name": name,
                        "tourid": tourid
                    })
                    alrea.append(seriesid)

        Series = {
            "id": seriesid,
            "name": name,
            "status": Series_raw.get("Status"),
            "matchcount": Series_raw.get("Matchcount"),
            "islastmatch": Series_raw.get("Is_series_lastmatch")

        }

        Tour = {
            "id": tourid,
            "name": tourname
        }

        Venue_raw = maindetail.get("Venue", {})
        vname = None
        vcountry = None
        vcity = None
        Weather = {}
        Pitch = {}

        if Venue_raw:
            venid_raw = Venue_raw.get("Id")
            vname = Venue_raw.get("Name")
            vcountry = Venue_raw.get("Country")
            vcity = Venue_raw.get("City")
            latitude = Venue_raw.get("Latitude")
            longitude = Venue_raw.get("Longitude")

            if venid_raw is not None:
                vid = idmap(int(venid_raw))
                if vid not in alrea:
                    venues.append({
                        "id": vid,
                        "name": vname,
                        "country": vcountry,
                        "city": vcity,
                        "latitude": latitude,
                        "longitude": longitude
                    })
                    alrea.append(vid)

            weathehr = Venue_raw.get("Venue_Weather", {})
            wname = weathehr.get("Weather")
            desc = weathehr.get("Description")
            humidity = weathehr.get("Humidity")
            Temp = weathehr.get("Temperature")
            visib = weathehr.get("Visibility")
            wind = weathehr.get("Wind_Speed")

            Weather = {
                "Weather": maindetail.get("Weather") if maindetail.get("Weather") is not None else wname,
                "Description": desc,
                "Humidity": humidity,
                "Temperature": Temp,
                "Visibility": visib,
                "Wind": wind
            }

            pitch = Venue_raw.get("Pitch_Detail", {})
            Pitch = {
                "Suited_For": pitch.get("Pitch_Suited_For"),
                "Surface": pitch.get("Pitch_Surface"),
            }

        Venue = {
            "name": vname,
            "country": vcountry,
            "city": vcity,
            "isneutral": Venue_raw.get("Neutralvenue")
        }

        toss_won_raw = maindetail.get("Tosswonby")
        Toss = {
            "won": idmap(int(toss_won_raw)) if toss_won_raw is not None else None,
            "decision": maindetail.get("Toss_elected_to")
        }

        raw_matchresult = maindetail.get("Raw_matchresult")
        res_str = None
        if raw_matchresult == "r":
            res_str = "By Runs"
        elif raw_matchresult == "w":
            res_str = "By Wickets"
        elif raw_matchresult is not None:
            res_str = "Tie"

        officials = maindetail.get("Officials", {})
        Officials = {}
        Officials = {
            "umpire1": idmap(int(officials.get("Umpire1_Id"))) if officials.get("Umpire1_Id") else None,
            "umpire2": idmap(int(officials.get("Umpire2_Id"))) if officials.get("Umpire2_Id") else None,
            "umpire3": idmap(int(officials.get("Umpire3_Id"))) if officials.get("Umpire3_Id") else None,
            "referee": idmap(int(officials.get("Referee_Id"))) if officials.get("Referee_Id") else None,
            "umpirestext": officials.get("Umpires")
        }

        registry[Officials["umpire1"]] = officials.get("Umpire1_Name")
        registry[Officials["umpire2"]] = officials.get("Umpire2_Name")
        registry[Officials["umpire3"]] = officials.get("Umpire3_Name")
        registry[Officials["referee"]] = officials.get("Referee")

        winteam_raw = maindetail.get("Winningteam")
        Outcome = {
            "result": res_str,
            "WinTeam": idmap(int(winteam_raw)) if winteam_raw is not None else None,
            "Winmargin": maindetail.get("Winmargin"),
            "statement": maindetail.get("Equation"),
        }

        In = d.get("Innings", [])
        len_in = len(In)
        Summary = {}
        if len(In) > 0 and In[0].get('Total') is not None:
            Summary["1stInningTotal"] = f"{In[0].get('Total')}/{In[0].get('Wickets')}  ({In[0].get('Overs')})"
        if len(In) > 1 and In[1].get('Total') is not None:
            Summary["2ndInningTotal"] = f"{In[1].get('Total')}/{In[1].get('Wickets')}  ({In[1].get('Overs')})"

        alalal = d.get("Matchequation", {})
        t1_name = alalal.get('Team1_name') or alalal.get('Team1_Name')
        t2_name = alalal.get('Team2_Name') or alalal.get('Team2_name')
        background = {
            t1_name: t1id,
            t2_name: t2id
        }
        # if t1_name is not None:
        #     background[t1_name] = t1id
        # if t2_name is not None:
        #     background[t2_name] = t2id

        basics = {
            "id": id,
            "background": background if background else None,
            "date": date,
            "type": type,
            "total_innings": len_in,
            "number": num,
            "daynight": daynight,
            "league": League,
            "series": Series,
            "tours": Tour,
            "venue": Venue,
            "weather": Weather,
            "pitch": Pitch,
            "toss": Toss,
            "officials": Officials,
            "outcome": Outcome,
            "summary": Summary,
            "awards": Awards if Awards else None
        }

        # basics = remove_nones(basics)

    return basics


def ballbyball(matchid, inning, players_registry):
    # with open(f"./rawdata/matches/{matchid}/{inning}/commentary.json", "r") as f:
    if not os.path.exists(f"{MATCHES}/{matchid}/{inning}/commentary.json"):
            noBbBd.append(
                {
                    "matchid": matchid,
                    "inning": inning
                }
            )
            return {}
    with open(f"{MATCHES}/{matchid}/{inning}/commentary.json", "r", encoding="utf-8") as f:
        # with open(f"./today/rawmatches/{matchid}/{inning}/commentary.json", "r") as f:
        data = json.load(f)

    # data = raw_data.get("data")
    if not data:
        return None

    commentary_list = data.get("Commentary", [])

    parsed_balls = []
    total_deliveries = 0
    legal_deliveries = 0

    for i,ball in enumerate(commentary_list):
        total_deliveries += 1

        detail = ball.get("Detail", "").lower()
        is_legal = True
        if "wd" in detail or "nb" in detail:
            is_legal = False

        if is_legal:
            legal_deliveries += 1

        bowler_id_raw = ball.get("Bowler")
        batsman_id_raw = ball.get("Batsman")
        non_striker_id_raw = ball.get("Non_Striker")
        dismissed_id_raw = ball.get("Dismissed")

        bowler_id = idmap(int(bowler_id_raw)) if bowler_id_raw else None
        batsman_id = idmap(int(batsman_id_raw)) if batsman_id_raw else None
        non_striker_id = idmap(int(non_striker_id_raw)) if non_striker_id_raw else None
        dismissed_id = idmap(int(dismissed_id_raw)) if dismissed_id_raw else None

        if bowler_id and ball.get("Bowler_Name"):
            players_registry[bowler_id] = ball.get("Bowler_Name")
        if batsman_id and ball.get("Batsman_Name"):
            players_registry[batsman_id] = ball.get("Batsman_Name")
        if non_striker_id and ball.get("Non_Striker_Name"):
            players_registry[non_striker_id] = ball.get("Non_Striker_Name")

        fielders_raw = ball.get("Fielders", [])
        parsed_fielders = []
        for f in fielders_raw:
            fid_raw = f.get("Player_Id")
            if fid_raw:
                fid = idmap(int(fid_raw))
                players_registry[fid] = f.get("Player_Name")
                parsed_fielders.append(fid)
        # i =0
        # zad = ball.get("ZAD"," , , , ")
        # print(zad)
        # if zad:
        #     z,a,d = zad.split(",")
        # else:
        #     z,a,d = None
        
        if ball.get("Over"):
            parsed_ball = {
                "id" : f"{matchid}{inning}{ndigitpad(inning,3)}{ndigitpad(i,2)}",
                "over": ball.get("Over"),
                "over_no": int(ball.get("Over_No")) if ball.get("Over_No") else None,
                "ball_no": int(ball.get("Ball")) if ball.get("Ball") else None,
                "runs": int(ball.get("Runs")) if ball.get("Runs") else 0,
                "batter_runs": int(ball.get("Batsman_Runs")) if ball.get("Batsman_Runs") else 0,
                "bowler_conceded_runs": int(ball.get("Bowler_Conceded_Runs")) if ball.get(
                    "Bowler_Conceded_Runs") else 0,
                "extras_runs": int(ball.get("Extras_Runs")) if ball.get("Extras_Runs") else 0,
                "detail": ball.get("Detail"),
                "is_legal": is_legal,
                "is_boundary": ball.get("Isboundary"),
                "is_wicket": ball.get("Iswicket"),
                "bowler": bowler_id,
                "batter": batsman_id,
                "non_striker": non_striker_id,
                "dismissed": dismissed_id,
                "dismissal_type": ball.get("Dismissal_Type"),
                "dismissal_id": ball.get("Dismissal_Id"),
                "howout": ball.get("Howout"),
                "fielders": parsed_fielders,
                "zad": ball.get("ZAD",None),
                "ball_speed": ball.get("Ball_Speed"),
                "ball_line_length": ball.get("Ball_Line_Length"),
                "shot_type_id": ball.get("Shot_Type_Id"),
                "shot_type": ball.get("Shot_Type")
            }

            parsed_balls.append(parsed_ball)
        # i+=1

    parsed_balls.reverse()

    batting_team_raw = data.get("BattingTeam_Id")
    bowling_team_raw = data.get("BowlingTeam_Id")

    inning_data = {
        "inning_no": int(data.get("InningNo")) if data.get("InningNo") else inning,
        "inninginfo": {"batting_team": idmap(int(batting_team_raw)) if batting_team_raw else None,
                       "bowling_team": idmap(int(bowling_team_raw)) if bowling_team_raw else None,
                       "total_deliveries": total_deliveries,
                       "legal_deliveries": legal_deliveries},
        "balls": parsed_balls
    }

    # return remove_nones(inning_data) 
    return inning_data # got fucked last time after doing data anaklysis wjere structire fuicked yp coz of not "none"


def cricdata(matchid):
    if already_exist(matchid):
        return
    players_registry = {}

    basic = parsematch(matchid, players_registry)
    # print(basic)
    Inn = {}
    innno = basic.get("total_innings", 0)
    for i in range(1, innno + 1):
        Inn[f"inning{i}"] = ballbyball(matchid, i, players_registry)
    D = {
        "info": {
            "project" : "cricdata",
            
            "version" : "1.0.0",
            "author" : "RAJ",  # credits lena hai
            "date of parsing" : date.today().strftime("%d/%m/%Y"),
            "license" : "MIT",
            "github" : "https://github.com/rajahmed2007",
            "credits" : None  #koi nahi
        },
        "match":{
        "basic": basic,
        "Innings": Inn,
        "Registry": players_registry
        }
    }

    with open(f"{OUTDIR}/{matchid}.json", "w") as outfile:
        json.dump(D, outfile,indent=4)
###################################################################################
###################################################################################
# IMPLEMENTATION
###################################################################################
# cricdata(1083691212)

t1 = time.time()
L = os.listdir(MATCHES)
# print(L[2])
tasks = list(map(int, L))
#
# tasks = [3716920,4058253]
print(f"Got {len(tasks)} Matches")
with ThreadPoolExecutor(max_workers=80) as executor:
    with tqdm.tqdm(total=len(tasks)) as pbar:
        futures = {
            executor.submit(cricdata, m) for m in tasks
        }

        for future in as_completed(futures):
            # for count, future in enumerate(futures):
            # print(count, end="\r")
            try:
                future.result()
            except Exception as e:
#                 # print(f"Error processing match {future}: {e}")
                with open("../logs/matchparsing.log", "a") as l:
                    l.write(traceback.format_exc())
            finally:
                pbar.update(1)
    executor.shutdown()
# Leagues = []
# serieses = []
# tours = []
# venues = []


df1 = pd.DataFrame(serieses)
df2 = pd.DataFrame(tours)
df3 = pd.DataFrame(venues)
df4 = pd.DataFrame(Leagues)
df5 = pd.DataFrame(noBbBd)
# df2 = pd.concat([pd.read_parquet(f"./{dis}/tours.parquet"),df2]).drop_duplicates()
# df1 = pd.concat([pd.read_parquet(f"./{dis}/serieses.parquet"),df1]).drop_duplicates()
# df3 = pd.concat([pd.read_parquet(f"./{dis}/venues.parquet"),df3]).drop_duplicates()
# df4 = pd.concat([pd.read_parquet(f"./{dis}/leagues.parquet"),df4]).drop_duplicates()
# df5 = pd.concat([pd.read_parquet(f"./{dis}/NoBallbyBall.parquet"),df5]).drop_duplicates()
df1.to_parquet(f"./{dis}/serieses.parquet")
df2.to_parquet(f"./{dis}/tours.parquet")
df3.to_parquet(f"./{dis}/venues.parquet")
df4.to_parquet(f"./{dis}/leagues.parquet")
df5.to_parquet(f"./{dis}/NoBallbyBall.parquet")

t2 = time.time()
print(f"Time taken: {t2 - t1} seconds")