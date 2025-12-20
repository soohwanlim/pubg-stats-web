import requests
import os
import json
from datetime import datetime
import time
import sqlite3

DB_FILE = "leaderboard.db"

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS leaderboard_meta (
                    platform TEXT, 
                    mode TEXT, 
                    updated_at REAL,
                    PRIMARY KEY (platform, mode)
                )''')
    c.execute('''CREATE TABLE IF NOT EXISTS leaderboard_players (
                    platform TEXT,
                    mode TEXT,
                    rank INTEGER,
                    nickname TEXT,
                    tier TEXT,
                    rp INTEGER,
                    avg_dmg INTEGER,
                    kd REAL,
                    avg_kills REAL,
                    wins INTEGER,
                    win_rate TEXT,
                    games INTEGER
                )''')
    
    # New Table for Player Stats Caching
    c.execute('''CREATE TABLE IF NOT EXISTS player_stats (
                    nickname TEXT,
                    platform TEXT,
                    json_data TEXT,
                    updated_at REAL,
                    PRIMARY KEY (nickname, platform)
                )''')
                
    conn.commit()
    conn.close()

# Initialize DB on load
init_db()

# API Key Loading
# Priority: 1. Environment Variable (Render/Cloud) 2. Local File (Dev)
API_KEY = os.environ.get("PUBG_API_KEY")

if not API_KEY:
    try:
        with open('PUBG_API_KEY.txt', 'r') as f:
            API_KEY = f.read().strip()
    except FileNotFoundError:
        print("Warning: PUBG_API_KEY.txt not found and env var not set.")
        API_KEY = ""

HEADER = {
    "Authorization": f"Bearer {API_KEY}",
    "Accept": "application/vnd.api+json"
}


def get_api_url(shard="steam"):
    return f"https://api.pubg.com/shards/{shard}"

# BASE_URL removed in favor of get_api_url()



def save_leaderboard_to_db(platform, mode, data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    # 1. Update Meta
    c.execute("INSERT OR REPLACE INTO leaderboard_meta (platform, mode, updated_at) VALUES (?, ?, ?)",
              (platform, mode, time.time()))
    
    # 2. Clear old data for this shard/mode
    c.execute("DELETE FROM leaderboard_players WHERE platform=? AND mode=?", (platform, mode))
    
    # 3. Insert new data
    for p in data:
        c.execute('''INSERT INTO leaderboard_players VALUES (
                        ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
                     )''', (
                         platform, mode, p['rank'], p['nickname'], p['tier'], p['rp'],
                         p['avg_dmg'], p['kd'], p['avg_kills'], p['wins'], p['win_rate'], p['games']
                     ))
    conn.commit()
    conn.close()

def load_leaderboard_from_db(platform, mode):
    conn = sqlite3.connect(DB_FILE)
    conn.row_factory = sqlite3.Row # Access columns by name
    c = conn.cursor()
    
    # Check Meta
    c.execute("SELECT updated_at FROM leaderboard_meta WHERE platform=? AND mode=?", (platform, mode))
    row = c.fetchone()
    if not row:
        conn.close()
        return None
        
    updated_at = row['updated_at']
    
    # Check if "On the hour" has passed
    # Logic: If saved time's hour != current time's hour, refresh.
    # Also considering date change.
    saved_dt = datetime.fromtimestamp(updated_at)
    now_dt = datetime.now()
    
    # If it's a different hour (e.g. 13:59 vs 14:00) -> Expire
    # Or if it's too old (> 1 hour) just in case
    if saved_dt.hour != now_dt.hour or saved_dt.date() != now_dt.date():
        conn.close()
        return None # Expired (New hour started)
        
        
    # Fetch Players
    c.execute("SELECT * FROM leaderboard_players WHERE platform=? AND mode=? ORDER BY rank ASC", (platform, mode))
    rows = c.fetchall()
    
    result = []
    for r in rows:
        result.append({
            "rank": r['rank'],
            "nickname": r['nickname'],
            "tier": r['tier'],
            "rp": r['rp'],
            "avg_dmg": r['avg_dmg'],
            "kd": r['kd'],
            "avg_kills": r['avg_kills'],
            "wins": r['wins'],
            "win_rate": r['win_rate'],
            "games": r['games']
        })
    
    conn.close()
    return result, updated_at

def get_leaderboard(platform='steam', mode='squad-fpp'):
    """
    Get Leaderboard (Top 500) with 24h caching using SQLite.
    Returns: (list, timestamp)
    """
    # 1. Check Cache (DB)
    cached = load_leaderboard_from_db(platform, mode)
    if cached:
        data, updated_at = cached
        print(f"[DB] Loaded Leaderboard for {platform} from SQLite. (Time: {updated_at})")
        return data, updated_at
        
    # 2. Fetch from API
    # Need Season ID
    season_id = get_current_season_id(platform)
    
    # API: /shards/{platform}/leaderboards/{seasonId}/{gameMode}
    # API: /shards/{platform}/leaderboards/{seasonId}/{gameMode}
    # Fix: Leaderboards require specific regional shards, not generic 'steam'/'kakao'
    lb_shard = platform
    if platform == 'steam':
        lb_shard = 'pc-as' # Default to AS server for KR users
    elif platform == 'kakao':
        lb_shard = 'pc-kakao'
        
    url = f"https://api.pubg.com/shards/{lb_shard}/leaderboards/{season_id}/{mode}"
    
    try:
        print(f"[API] Fetching Leaderboard: {url}")
        res = requests.get(url, headers=HEADER, timeout=10)
        
        if res.status_code == 200:
            raw_data = res.json()
            included = raw_data.get('included', []) # Often players info
            # The structure of leaderboard usually has 'data' -> 'attributes' -> 'rank'
            # But official documentation says: data: { type: "leaderboard", attributes: { gameMode, shardId }, relationships: { players: { data: [] } } }
            # Actually the response 'included' contains the player objects with stats.
            
            # Let's inspect typical structure for leaderboard. 
            # It returns a list of players in 'included' usually with 'attributes' -> 'rank', 'stats'
            
            leaderboard_list = []
            
            # Based on documentation and typical response:
            # The 'included' array has objects of type 'player' or 'leaderboardParticipant'.
            # Usually it gives a list.
            
            # Actually, standard response:
            # { "data": { "type": "leaderboard", "attributes": {}, "relationships": { "players": { "data": [ { "type": "player", "id": "..." } ] } } }, "included": [ ... ] }
            # The stats like rank, kda are in 'included' objects or 'attributes'.
            
            # Let's assume 'included' contains the detailed stats.
            # However, for leaderboard endpoint, valid structure is:
            # attributes: { rank, name, stats: { ... } }
            
            # Wait, checking official docs:
            # GET /shards/{shardId}/leaderboards/{seasonId}/{gameMode}
            # Response: data: { type: "leaderboard", ... }, included: [ { type: "user", id: "...", attributes: { name: "...", rank: 1, stats: { ... } } } ]
            
            # IMPORTANT: The 'included' list has the actual ranking info.
            
            if 'included' in raw_data:
                for item in raw_data['included']:
                    attrs = item.get('attributes', {})
                    stats = attrs.get('stats', {})
                    
                    # Calculate simple K/D
                    # Note: Leaderboard stats might be slightly different key names
                    # Calculate K/D and Win Rate manually as API returns 0 or missing for leaderboard
                    games = stats.get('games', 0)
                    wins = stats.get('wins', 0)
                    kills = stats.get('kills', 0)
                    
                    deaths = games - wins
                    if deaths <= 0: deaths = 1 
                    
                    kd = round(kills / deaths, 2)
                    
                    # Avg Kills (Kills per Game) - More accurate for Leaderboard as we lack Death count
                    avg_kills = 0
                    if games > 0:
                        avg_kills = round(kills / games, 2)
                    
                    win_rate = "0.0%"
                    if games > 0:
                        win_rate = f"{(wins / games) * 100:.1f}%"

                    # Tier Logic (Prefer API value)
                    rp = stats.get('rankPoints', 0)
                    api_tier = stats.get('tier')
                    
                    if api_tier:
                        tier_name = api_tier
                        # API returns 'Master', 'Diamond', etc.
                    else:
                        # Fallback
                        tier_name = "Survivor"
                        if rp >= 3500: tier_name = "Master"
                        elif rp >= 3000: tier_name = "Diamond"
                        elif rp >= 2500: tier_name = "Platinum"
                        elif rp >= 2000: tier_name = "Gold"
                        elif rp >= 1500: tier_name = "Silver"
                        else: tier_name = "Bronze"

                    leaderboard_list.append({
                        "rank": attrs.get('rank', 0),
                        "nickname": attrs.get('name', 'Unknown'),
                        "tier": tier_name,
                        "rp": rp,
                        "avg_dmg": int(stats.get('averageDamage', 0)),
                        "kd": kd,
                        "avg_kills": avg_kills,
                        "wins": wins,
                        "win_rate": win_rate,
                        "games": games
                    })
                
                # Sort by rank just in case
                leaderboard_list.sort(key=lambda x: x['rank'])
                
                # Limit to Top 100
                leaderboard_list = leaderboard_list[:100]
                
                # Save to Cache (DB)
                save_leaderboard_to_db(platform, mode, leaderboard_list)
                print(f"[DB] Saved Leaderboard for {platform} to SQLite.")
                    
                return leaderboard_list, time.time()

    except Exception as e:
        print(f"[API Error] Leaderboard fetch failed: {e}")
        
    return [], 0


def get_player_account(nickname, shard='steam'):
    """
    Get player account info by nickname.
    """
    base_url = get_api_url(shard)
    url = f"{base_url}/players?filter[playerNames]={nickname}"
    try:
        res = requests.get(url, headers=HEADER, timeout=5)
        if res.status_code == 200:
            data = res.json()
            # return the first match
            return data['data'][0]
        elif res.status_code == 404:
            return None
        else:
            print(f"[API Error] get_player_account({shard}): {res.status_code} {res.text}")
            return None
    except Exception as e:
        print(f"[Sys Error] {e}")
        return None

def get_match_detail(match_id, shard='steam'):
    """
    Get detailed info for a single match.
    """
    base_url = get_api_url(shard)
    url = f"{base_url}/matches/{match_id}"
    try:
        res = requests.get(url, headers=HEADER, timeout=5)
        if res.status_code == 200:
            return res.json()
        else:
            print(f"[API Error] get_match_detail({shard}): {res.status_code}")
            return None
    except Exception as e:
        print(f"[Sys Error] {e}")
        return None

# Cache season ID to avoid re-fetching every time
CURRENT_SEASON_ID = None

def get_current_season_id(shard='steam'):
    global CURRENT_SEASON_ID
    if CURRENT_SEASON_ID:
        return CURRENT_SEASON_ID
    
    base_url = get_api_url(shard)
    url = f"{base_url}/seasons"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/vnd.api+json"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()['data']
            for season in data:
                # Find the current PC season (usually has isCurrentSeason=true)
                if season['attributes'].get('isCurrentSeason', False) and season['id'].startswith('division.bro.official.pc-2018'):
                    CURRENT_SEASON_ID = season['id']
                    return CURRENT_SEASON_ID
            # Fallback if not found explicitly (take the last one)
            CURRENT_SEASON_ID = data[-1]['id']
            return CURRENT_SEASON_ID
    except Exception as e:
        print(f"Error fetching seasons: {e}")
    return "division.bro.official.pc-2018-33" # Fallback hardcoded just in case

def get_ranked_stats(account_id, season_id, shard='steam'):
    base_url = get_api_url(shard)
    url = f"{base_url}/players/{account_id}/seasons/{season_id}/ranked"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/vnd.api+json"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()['data']['attributes']['rankedGameModeStats']
            return data.get('squad', None) # Return Squad stats
    except:
        pass
    return None

def get_season_stats(account_id, season_id, shard='steam'):
    base_url = get_api_url(shard)
    url = f"{base_url}/players/{account_id}/seasons/{season_id}"
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Accept": "application/vnd.api+json"
    }
    try:
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()['data']['attributes']['gameModeStats']
            return data.get('squad', None) # Return Squad stats
    except:
        pass
    return None

def get_notices():
    """
    Fetch PUBG News from Steam API.
    AppID: 578080 (PUBG)
    """
    url = "http://api.steampowered.com/ISteamNews/GetNewsForApp/v0002/?appid=578080&count=5&maxlength=300&format=json"
    
    try:
        res = requests.get(url, timeout=5)
        if res.status_code != 200:
            return []
            
        data = res.json()
        news_items = data.get('appnews', {}).get('newsitems', [])
        
        result = []
        for item in news_items:
            # Convert timestamp to YYYY.MM.DD
            ts = item.get('date', 0)
            date_str = datetime.fromtimestamp(ts).strftime('%Y.%m.%d')
            
            result.append({
                'title': item.get('title', 'No Title'),
                'date': date_str,
                'link': item.get('url', '#')
            })
            
        return result

    except Exception as e:
        print(f"Steam News Error: {e}")
        return []


def calculate_ai_score(rank, kills, damage, time_survived):
    # Weights
    W_RANK = 0.2
    W_TIME = 0.1
    W_KILL = 0.35
    W_DMG = 0.35

    # Max reference values
    MAX_PLAYERS = 64 # Squad is usually around 64-100, let's assume team rank. 
    # Actually rank input is team rank (1~20+). Let's say max team rank is 25 (100/4).
    # User's logic: norm_rank = (MAX_PLAYERS - rank) / (MAX_PLAYERS - 1). 
    # If using team rank (1-30), MAX_PLAYERS should be around 30.
    # User code snippet used MAX_PLAYERS = 31.
    MAX_TEAMS = 31
    MAX_KILLS = 10
    MAX_DMG = 700
    MAX_TIME = 1500 # User said 3000(30m), but 1500(25m) is more realistic for active time? User said 3000.
    # User Request: MAX_TIME = 3000
    
    # Normalization
    norm_rank = max(0.0, (MAX_TEAMS - rank) / (MAX_TEAMS - 1))
    norm_time = min(time_survived / 3000, 1.0) # User said 3000
    norm_kill = min(kills / MAX_KILLS, 1.0)
    norm_dmg = min(damage / MAX_DMG, 1.0)

    final_score = (norm_rank * W_RANK) + (norm_time * W_TIME) + (norm_kill * W_KILL) + (norm_dmg * W_DMG)
    
    # 1.5x Multiplier for 1st Place
    if rank == 1:
        final_score *= 1.5
        
    score_int = int(round(final_score * 100))
    
    return score_int

def determine_tags(avg_time_str, avg_kills, avg_dmg, win_rate_str):
    tags = []
    
    # Parse avg_time "15m 30s" -> seconds
    # avg_time comes from calc_sum which returns formatted string "Xm Ys"
    try:
        m_part = int(avg_time_str.split('m')[0])
        s_part = int(avg_time_str.split('m')[1].strip().replace('s',''))
        total_sec = m_part * 60 + s_part
    except:
        total_sec = 0

    # Parse win_rate "30.0%" -> float
    try:
        wr = float(win_rate_str.replace('%',''))
    except:
        wr = 0.0

    # 1. 초반 교전: Avg Time < 5m (300s)
    if total_sec > 0 and total_sec < 300:
        tags.append({"text": "초반교전", "color": "#ef4444"}) # Red
    
    # 3. 여포: Avg Kills > 7
    # 4. 학살자: Avg Time > 15m (900s) AND Avg Kills > 7
    if avg_kills > 7:
        if total_sec > 900:
            tags.append({"text": "학살자", "color": "#8b5cf6"}) # Purple
        else:
            tags.append({"text": "여포", "color": "#f97316"}) # Orange
    else:
        # 2. 존버: Avg Time > 15m (900s) AND Avg Kills <= 2
        if total_sec > 900 and avg_kills <= 2:
            tags.append({"text": "존버", "color": "#14b8a6"}) # Teal

    # 5. 치킨: Win Rate >= 30%
    if wr >= 30.0:
        tags.append({"text": "치킨", "color": "#eab308"}) # Yellow (Gold)

    # 6. 딜: Avg Dmg >= 500
    if avg_dmg >= 500:
        tags.append({"text": "딜", "color": "#ec4899"}) # Pink

    return tags


def save_player_stats_to_db(nickname, platform, data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    import json
    
    # Store the dictionary as a JSON string
    json_str = json.dumps(data, ensure_ascii=False)
    
    c.execute("INSERT OR REPLACE INTO player_stats (nickname, platform, json_data, updated_at) VALUES (?, ?, ?, ?)",
              (nickname, platform, json_str, time.time()))
    
    conn.commit()
    conn.close()

def load_player_stats_from_db(nickname, platform):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    c.execute("SELECT json_data, updated_at FROM player_stats WHERE nickname=? AND platform=?", (nickname, platform))
    row = c.fetchone()
    conn.close()
    
    if row:
        import json
        data = json.loads(row[0])
        return data, row[1]
    return None, 0

def update_player_stats(nickname, platform='steam'):
    """
    Force update player stats from API.
    Check cooldown (30 min).
    """
    # 1. Check Cooldown
    _, updated_at = load_player_stats_from_db(nickname, platform)
    if updated_at > 0:
        elapsed = time.time() - updated_at
        if elapsed < 300: # 5 mins = 300 sec
            remaining_min = int((300 - elapsed) / 60)
            return {"error": "Cooldown", "message": f"전적 갱신은 5분마다 가능합니다. (약 {remaining_min}분 남음)"}

    # 2. Fetch Fresh Data
    # We reuse the logic inside get_recent_stats but bypass the cache check part,
    # or we can refactor. simpler is to make a private _fetch_stats function.
    # But for now, let's just make get_recent_stats support a 'force_update' flag or similar.
    # Actually, get_recent_stats below has the logic. Let's extract valid fetching logic.
    
    fresh_data = _fetch_stats_from_api(nickname, platform)
    
    if "error" in fresh_data:
        return fresh_data
        
    # 3. Save
    save_player_stats_to_db(nickname, platform, fresh_data)
    return {"success": True}

def _fetch_stats_from_api(nickname, platform):
    """
    Internal function to fetch fresh data from API (logic moved from get_recent_stats)
    """
    # Use selected platform directly
    target_shard = platform
    account = get_player_account(nickname, target_shard)
        
    if not account:
        return {"error": "UserNotFound"}
    
    account_id = account['id']
    matches = account['relationships']['matches']['data']
    
    # ... (Rest of the logic from old get_recent_stats) ...
    # Copying existing logic here is verbose. 
    # Let's Refactor get_recent_stats to simply call this.
    pass # Replaced below

def get_recent_stats(nickname, platform='steam'):
    """
    High-level function to get stats.
    Read from DB if exists. If not, fetch from API.
    """
    # 1. Try DB
    cached_data, updated_at = load_player_stats_from_db(nickname, platform)
    if cached_data:
        # Inject timestamp for UI
        cached_data['updated_at'] = updated_at
        return cached_data

    # 2. Fetch API
    data = _fetch_stats_from_api(nickname, platform)
    
    if "error" not in data:
        save_player_stats_to_db(nickname, platform, data)
        data['updated_at'] = time.time()
        
    return data

def _fetch_stats_from_api(nickname, platform):
    target_shard = platform
    account = get_player_account(nickname, target_shard)
        
    if not account:
        return {"error": "UserNotFound"}
    
    print(f"User {nickname} found in {target_shard} (API Fetch)")
    
    account_id = account['id']
    matches = account['relationships']['matches']['data']
    
    # 1. Recent Matches Processing
    match_data_list = []

    # Accumulators for Recent 10 (Side Box)
    stats_recent = {
        'all': {'k':0, 'd':0, 'dmg':0, 'wins':0, 'top10':0, 'rank_sum':0, 'time_sum':0, 'ai_score_sum':0, 'cnt':0}
    }

    MAX_SEARCH_DEPTH = 60 # Check up to 60 recent matches to find 10 squad games
    for m in matches[:MAX_SEARCH_DEPTH]:
        if len(match_data_list) >= 10:
            break
            
        m_id = m['id']
        full_data = get_match_detail(m_id, target_shard)
        if not full_data: 
            continue
        
        attributes = full_data['data']['attributes']
        map_name = attributes['mapName']
        game_mode = attributes['gameMode']
        match_type = attributes.get('matchType', 'official')
        created_at = attributes['createdAt']
        
        if 'squad' not in game_mode:
            continue

        is_ranked = (match_type == 'competitive')
        mode_display = "Ranked" if is_ranked else "Normal"
        mode_display += " Squad"

        included = full_data.get('included', [])
        participants_map = {item['id']: item for item in included if item['type'] == 'participant'}

        user_participant_id = None
        user_stats = None
        for pid, p_data in participants_map.items():
            if p_data['attributes']['stats']['playerId'] == account_id:
                user_participant_id = pid
                user_stats = p_data['attributes']['stats']
                break
        if not user_stats: continue

        team_members = []
        team_rank = user_stats['winPlace']
        
        for item in included:
            if item['type'] == 'roster':
                roster_p_list = [p['id'] for p in item['relationships']['participants']['data']]
                if user_participant_id in roster_p_list:
                    team_rank = item['attributes']['stats']['rank']
                    for member_pid in roster_p_list:
                        if member_pid in participants_map:
                            m_stat = participants_map[member_pid]['attributes']['stats']
                            # ai_score = random.choice(['S', 'A+', 'A', 'B+', 'B', 'C', 'D'])
                            score_val = calculate_ai_score(
                                team_rank, 
                                m_stat['kills'], 
                                int(m_stat['damageDealt']), 
                                m_stat['timeSurvived']
                            )
                            team_members.append({
                                "name": m_stat['name'],
                                "kills": m_stat['kills'],
                                "damage": int(m_stat['damageDealt']),
                                "survived": f"{m_stat['timeSurvived']//60}:{m_stat['timeSurvived']%60:02d}",
                                "is_me": (member_pid == user_participant_id),
                                "ai_score": score_val
                            })
                    break
        team_members.sort(key=lambda x: (x['is_me'], x['damage']), reverse=True)

        k = user_stats['kills']
        d = 1 if user_stats['deathType'] != 'alive' else 0
        dmg = user_stats['damageDealt']
        survived = user_stats['timeSurvived']
        
        stats_recent['all']['cnt'] += 1
        stats_recent['all']['k'] += k
        stats_recent['all']['d'] += d
        stats_recent['all']['dmg'] += dmg
        stats_recent['all']['time_sum'] += survived
        stats_recent['all']['rank_sum'] += team_rank
        if team_rank == 1: stats_recent['all']['wins'] += 1

        # Find my score
        my_score = 0
        for member in team_members:
            if member['is_me']:
                my_score = member['ai_score']
                break
        
        stats_recent['all']['ai_score_sum'] += my_score

        match_data_list.append({
            "map": get_map_display(map_name),
            "mode": mode_display,
            "is_ranked": is_ranked,
            "date": created_at[:10],
            "place": team_rank,
            "kills": k,
            "damage": int(dmg),
            "time": f"{survived//60}m",
            "ai_score": my_score,
            "team": team_members
        })

    # Helper for Recent 10
    def calc_sum(s):
        if s['cnt'] == 0: return {'kd':0, 'avg_dmg':0, 'wins':0, 'avg_rank':'-', 'avg_time':'-', 'avg_ai_score': '-', 'count':0, 'win_rate':'0.0%', 'avg_kills': 0}
        kd = s['k'] / s['d'] if s['d'] > 0 else s['k']
        win_rate = (s['wins'] / s['cnt']) * 100
        return {
            'kd': round(kd, 2),
            'avg_dmg': int(s['dmg'] / s['cnt']),
            'wins': s['wins'],
            'avg_rank': round(s['rank_sum'] / s['cnt'], 1),
            'avg_time': f"{int(s['time_sum']/s['cnt'])//60}m {int(s['time_sum']/s['cnt'])%60}s",
            'avg_ai_score': int(round(s['ai_score_sum'] / s['cnt'])),
            'count': s['cnt'],
            'win_rate': f"{win_rate:.1f}%",
            'avg_kills': round(s['k'] / s['cnt'], 1) if s['cnt'] > 0 else 0
        }

    # 2. Real Season Stats
    season_id = get_current_season_id(target_shard)
    
    # Ranked
    r_stats = get_ranked_stats(account_id, season_id, target_shard)
    ranked_summary = {
        "tier": "Unranked", "sub_tier": "", "rp": 0, "kd": 0.0, "avg_dmg": 0, "avg_rank": "-", "wins": 0
    }
    if r_stats:
        current_tier = r_stats.get('currentRankPoint', 0)
        
        tier_data = r_stats.get('currentTier', {})
        if tier_data:
            ranked_summary['tier'] = tier_data.get('tier', 'Unknown')
            ranked_summary['sub_tier'] = tier_data.get('subTier', '')
            ranked_summary['rp'] = r_stats.get('currentRankPoint', 0)
        
        deaths = r_stats.get('deaths', 0)
        kills = r_stats.get('kills', 0)
        damage = r_stats.get('damageDealt', 0)
        wins = r_stats.get('wins', 0)
        cnt = r_stats.get('roundsPlayed', 0)
        avg_rank = r_stats.get('avgRank', 0) 
        
        ranked_summary['kd'] = round(kills / deaths, 2) if deaths > 0 else kills
        ranked_summary['avg_dmg'] = int(damage / cnt) if cnt > 0 else 0
        ranked_summary['avg_rank'] = round(avg_rank, 1) if avg_rank else '-'
        ranked_summary['wins'] = wins
        ranked_summary['win_rate'] = f"{(wins/cnt)*100:.1f}%" if cnt > 0 else "0.0%"
        
        # Icon Logic
        tier_base = ranked_summary['tier']
        tier_clean = tier_base.strip().split()[0] 
        
        valid_tiers = ['Bronze', 'Silver', 'Gold', 'Platinum', 'Diamond', 'Master', 'Survivor', 'Crystal']
        if tier_clean in valid_tiers:
             ranked_summary['tier_icon'] = f"/static/images/tiers/{tier_clean}.png"
        else:
             ranked_summary['tier_icon'] = None

    # Normal

    n_stats = get_season_stats(account_id, season_id, target_shard)
    normal_summary = {"kd": 0.0, "avg_dmg": 0, "avg_rank": "-", "wins": 0}
    if n_stats:
        deaths = n_stats.get('losses', 0)
        kills = n_stats.get('kills', 0)
        damage = n_stats.get('damageDealt', 0)
        wins = n_stats.get('wins', 0)
        cnt = n_stats.get('roundsPlayed', 0)
        
        normal_summary['kd'] = round(kills / deaths, 2) if deaths > 0 else kills
        normal_summary['avg_dmg'] = int(damage / cnt) if cnt > 0 else 0
        normal_summary['wins'] = wins
        normal_summary['win_rate'] = f"{(wins/cnt)*100:.1f}%" if cnt > 0 else "0.0%"

    # Calculate Tags
    recent_summ = calc_sum(stats_recent['all'])
    tags = determine_tags(recent_summ['avg_time'], recent_summ['avg_kills'], recent_summ['avg_dmg'], recent_summ['win_rate'])

    return {
        "nickname": nickname,
        "platform": target_shard,
        "tags": tags,
        "matches": match_data_list,
        "recent_summary": recent_summ,
        "season_summary": {
            "ranked": ranked_summary,
            "normal": normal_summary
        },
        "tier_icon": ranked_summary.get('tier_icon', None)
    }

def get_map_display(raw_name):
    maps = {
        "Desert_Main": "Miramar",
        "DihorOtok_Main": "Vikendi",
        "Erangel_Main": "Erangel",
        "Baltic_Main": "Erangel",
        "Savage_Main": "Sanhok",
        "Summerland_Main": "Karakin",
        "Chimera_Main": "Paramo",
        "Heaven_Main": "Haven",
        "Tiger_Main": "Taego",
        "Kiki_Main": "Deston",
        "Neon_Main": "Rondo"
    }
    return maps.get(raw_name, raw_name)
