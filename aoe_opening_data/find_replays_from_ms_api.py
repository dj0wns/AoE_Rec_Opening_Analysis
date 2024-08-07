import queue
import requests
import threading
import time
import io
import zipfile
import heapq
import multiprocessing

import parse_replays_and_store_in_db


API_REQ_PER_MINUTE = 20
VERSION = 104954
NUM_PROCESSES = 10
# Keep our query size as large as possible to reduce strain one aoe.ms api
PLAYERS_PER_QUERY = 5
ELOS_PER_QUERY = 100
LEADERBOARD_IDS = [
  # leaderboard id : matchtype id
  # from https://aoe-api.worldsedgelink.com/community/leaderboard/getAvailableLeaderboards?title=age2
  (3, [6]), # RM 1v1
  #(4, [7,8,9]), # RM Team - internally we will use 402, 403, 404 for 2v2, 3v3, 4v4
  # EMPIRE WARS
  (13, [26]), #EW 1v1
  #(14, [27,28,29]), #EW Team
  # THESE ARENT SUPPORTED BY THE PARSER YET
  #(15, [66]), #Controller RM 1v1
  #(16, [67,68,69]), #Controller RM Team
]

processes = []

# send match_id, [(player_id, elo), (player_id, elo), ...], leaderboard_id
match_queue = multiprocessing.JoinableQueue()

def match_queue_consumer(queue, counter, errors):
  last_update = 0
  while True:
    try:
      item = queue.get()
    except queue.Empty:
      continue
    else:
      if last_update == 0:
        last_update = time.time_ns() /1000000;
      match_id = item[0]
      players = item[1]
      leaderboard_id = item[2]
      search_player = item[3]
      player_ids = [player["id"] for player in players]
      if search_player:
        # move search player to first in search list to maximize chance of finding
        player_ids.insert(0, player_ids.pop(player_ids.index(search_player)))
      else:
        print("No search player!!!")
      download_success = False
      for player_id in player_ids:
        # Wait until next update window
        now = time.time_ns() /1000000. # convert to millis
        time_to_wait = last_update + 60000./(API_REQ_PER_MINUTE/NUM_PROCESSES) - now
        if time_to_wait > 0:
          time.sleep(time_to_wait/1000.) # convert to seconds
        last_update = time.time_ns() /1000000.
        with counter.get_lock():
          counter.value  += 1
        print(f'processing item {item}, count: {counter.value}, errors: {errors.value}')
        if parse_replay_for_match(match_id, player_id, players, leaderboard_id):
          download_success = True
          break
        else:
          with errors.get_lock():
            errors.value  += 1
      if not download_success:
        print(f'Could not download/parse {match_id}')

      queue.task_done()

def parse_replay_for_match(match_id, player_id, players, leaderboard_id):
  try:
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}
    r = requests.get(
        f"https://aoe.ms/replay/?gameId={match_id}&profileId={player_id}",
        headers=headers)
  except Exception as e:
    return False
  if r.status_code == 404:
    # cant download match, add dummy to list of undownloadable matches
    print(f"Adding {match_id} to skip matches")
    parse_replays_and_store_in_db.connect_and_modify_with_list(
      [parse_replays_and_store_in_db.add_match(match_id, -1, -1, -1, -1, -1)])
    return False
  elif r.status_code != 200:
    print(f"Received {r.status_code} from {r.url}")
    return False
  # We found a valid map so unzip it
  try:
    replay_zip = zipfile.ZipFile(io.BytesIO(r.content))
    replay = replay_zip.read(replay_zip.namelist()[0])
    parse_replays_and_store_in_db.parse_replay_file(
        match_id, players,
        leaderboard_id, io.BytesIO(replay), VERSION)

  except Exception as e:
    print(e)
  return True # At this point the connection worked so any failure can't be retried

# takes an array of player_ids and fetches all of their match histories
def get_match_history_for_player_ids(player_ids):
  try:
    matches = requests.get(
        f'https://aoe-api.worldsedgelink.com/community/leaderboard/getRecentMatchHistory?title=age2&matchtype_id=8&profile_ids={player_ids}')
    print(matches.url)
    if matches.status_code != 200:
      return None
  except Exception as e:
    print(e)
    return None
  return matches.json()

# takes an array of player ids to resolve
def get_elo_for_player_ids(player_ids, leaderboard_id):
  try:
    stats = requests.get(
        f'https://aoe-api.worldsedgelink.com/community/leaderboard/getPersonalStat?title=age2&profile_ids={player_ids}')
    if stats.status_code != 200:
      return None
  except Exception as e:
    print(e)
    return None
  stats = stats.json()
  player_id_dict = {}
  player_stat_group_map = {}
  for stat_groups in stats["statGroups"]:
    for player in stat_groups["members"]:
      if player['profile_id'] not in player_stat_group_map:
        player_stat_group_map[player['personal_statgroup_id']] = \
            player['profile_id']
  for stat in stats["leaderboardStats"]:
    if stat["statgroup_id"] in player_stat_group_map and stat["leaderboard_id"] == leaderboard_id:
      player_id = player_stat_group_map[stat["statgroup_id"]]
      if player_id not in player_id_dict:
        #new player and elo to add
        player_id_dict[player_id] = stat["rating"]
  return player_id_dict

def get_leaderboard(leaderboard_id):
  try:
    leaderboard = requests.get(
        f'https://aoe-api.worldsedgelink.com/community/leaderboard/getLeaderBoard2?leaderboard_id={leaderboard_id}&title=age2')
    print(leaderboard.url)
    print(leaderboard.status_code)
    if leaderboard.status_code != 200:
      return None
  except Exception as e:
    print(e)
    return None
  return leaderboard.json()

def execute(leaderboard_ids):
  player_id_dict = {} # connects player_id to most recently observed elo
  player_id_heap = []
  # allows for bulk resolving of player elo
  player_processing_queue = queue.Queue() # match_id, [player_ids]
  added_matches_set = set() #set of added matches for quick lookups so we dont collide
  leaderboard_id = leaderboard_ids[0]
  matchtype_ids = leaderboard_ids[1]

  # Get starting player_id_set from top 200 on the aoe.ms api

  leaderboard = get_leaderboard(leaderboard_id)
  if leaderboard is None:
    return

  player_stat_group_map = {}
  for stat_group in leaderboard['statGroups']:
    for player in stat_group['members']:
      if player['profile_id'] not in player_stat_group_map:
        player_stat_group_map[player['personal_statgroup_id']] = \
            player['profile_id']
  for stat in leaderboard["leaderboardStats"]:
    if stat["statgroup_id"] in player_stat_group_map and stat['leaderboard_id'] == leaderboard_id:
      player_id = player_stat_group_map[stat["statgroup_id"]]
      if player_id not in player_id_dict:
        #new player and elo to add
        player_id_dict[player_id] = stat["rating"]
        heapq.heappush(player_id_heap, player_id)
    else:
      print (f'invalid stat group found: {stat["statgroup_id"]}')
  print (len(player_id_dict))

  # Now that we have players, lets move through the queue and find matches!
  while len(player_id_heap):
    player_ids = [heapq.heappop(player_id_heap)];
    for i in range(PLAYERS_PER_QUERY-1):
      # do multiple players at once to reduce query volume
      if len(player_id_heap) == 0:
        break
      player_ids.append(heapq.heappop(player_id_heap));
    matches = get_match_history_for_player_ids(player_ids)
    search_player_ids = player_ids.copy()
    if matches == None:
      continue
    for match in matches["matchHistoryStats"]:
      match_id = match["id"]
      if match["matchtype_id"] not in matchtype_ids:
        continue
      # sanity check that it is ranked
      if match["description"] != "AUTOMATCH":
        continue
      if not match["matchurls"]:
        # there are no replay records for this so skip it
        continue

      # ignore matches that are more than a week old
      current_time = round(time.time_ns() / 1000000000)
      # wait at least 5 minutes after match end to look for replay
      max_time = current_time - 300
      # any replay older than 7 days is probably long dead
      min_time = current_time - 168*60*60
      if match["completiontime"] < min_time or \
         match["completiontime"] > max_time:
          # replay file probably doesnt exist
          continue

      #get players and match id
      any_need_update = False
      search_player = False
      player_ids = []
      # add a specifier for winner id to look for their replay first
      for player in match["matchhistoryreportresults"]:
        player_id = player["profile_id"]
        player_ids.append(player_id)
        if not search_player and player_id in search_player_ids:
          search_player = player_id
        elif search_player and player_id in search_player_ids:
          # if both players are in just skip the replay... we have no way to determine whose replay it is and quite frankly its better to err on the side of fewer to avoid 404's
          continue
        if player_id not in player_id_dict:
          any_need_update = True
          player_id_dict[player_id] = -1 # to be updated later
          heapq.heappush(player_id_heap, player_id)

      #make sure match isnt already in db
      if parse_replays_and_store_in_db.does_match_exist(match_id):
        continue
      # make sure we havent already processed this match
      if match_id in added_matches_set:
        continue

      if not search_player:
        # maybe dont even try to grab this match as something may be awry
        print(player_ids, player_ids[0] in search_player_ids, player_ids[1] in search_player_ids, search_player_ids)
        print("No search player found in match?????")
      player_processing_queue.put((match_id, player_ids, search_player))
      added_matches_set.add(match_id)

    # now process the player_processing_queue and pop matches with all players into the matches_resolve queue
    players_to_resolve = []
    matches_to_resolve = queue.Queue() # (match_id, [player_ids])
    while not player_processing_queue.empty():
      item = player_processing_queue.get()
      match_id = item[0]
      player_ids = item[1]
      search_player = item[2]
      some_players_need_resolve = False
      for player in player_ids:
        if player_id_dict[player] < 0:
          some_players_need_resolve = True
          players_to_resolve.append(player)

      if not some_players_need_resolve:
        # both players exist so just fire off the match
        player_dict_array = [{"id":player, "elo":player_id_dict[player]} for player in player_ids]
        time.sleep(1) # sleep 500 ms to help spread out the parsers
        match_queue.put((match_id, player_dict_array, leaderboard_id, search_player))
        continue
      else:
        # we have a match needing resolution so store it in the resolve queue
        matches_to_resolve.put(item)


      # now if there are enough players in the players to resolve array, resolve them or the queue is empty
      if len(players_to_resolve) >= ELOS_PER_QUERY or player_processing_queue.empty():
        player_elos = get_elo_for_player_ids(players_to_resolve, leaderboard_id)
        if player_elos is not None:
          for player, elo in player_elos.items():
            player_id_dict[player] = elo
        players_to_resolve.clear() # clear the queue
        # now fire off matches to resolve
        while not matches_to_resolve.empty():
          item = matches_to_resolve.get()
          match_id = item[0]
          player_ids = item[1]
          search_player = item[2]
          # double check elos have been updated, else toss the match
          some_players_need_resolve = False
          for player in player_ids:
            if player_id_dict[player] < 0:
              some_players_need_resolve = True
              players_to_resolve.append(player)

          if some_players_need_resolve:
            continue
          #send the relevant data to the queue!
          player_dict_array = [{"id":player, "elo":player_id_dict[player]} for player in player_ids]
          time.sleep(1) # sleep 500 ms to help spread out the parsers
          match_queue.put((match_id, player_dict_array, leaderboard_id, search_player))

    print(len(player_id_heap),len(player_id_dict))
    print(match_queue.qsize())
    # sleep a bit to not flood api
    time.sleep(1)
    while match_queue.qsize() > 500:
      #sleep is queue is saturated so we dont hit the api too much
      time.sleep(10)

def close():
  for process in processes:
    process.kill()
  exit()

if __name__ == "__main__":
  counter = multiprocessing.Value('i', 0)
  errors = multiprocessing.Value('i', 0)
  # first lets fire off the consumers so we can parse replays as we get them
  for i in range(NUM_PROCESSES):
    p = multiprocessing.Process(target=match_queue_consumer, args=(match_queue,counter, errors))
    p.start()
    processes.append(p)

  while True:
    for i in LEADERBOARD_IDS:
      execute(i)
    close()
    # sleep 5 minutes on failed request to not flood the server
    time.sleep(300)
