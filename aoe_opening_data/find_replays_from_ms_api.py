import queue
import requests
import threading
import time
import io
import zipfile
import multiprocessing

import parse_replays_and_store_in_db

VERSION = 71094
NUM_PROCESSES = 16
# Keep our query size as large as possible to reduce strain one aoe.ms api
PLAYERS_PER_QUERY = 100
ELOS_PER_QUERY = 100
processes = []

# send match_id, (player_id, elo), (player_id, elo)
match_queue = multiprocessing.JoinableQueue()

def match_queue_consumer(queue):
  while True:
    try:
      item = queue.get()
    except queue.Empty:
      continue
    else:
      print(f'processing item {item}')
      match_id = item[0]
      player0 = item[1]
      player1 = item[2]
      parse_replay_for_match(match_id, player0, player1)
      queue.task_done()

def parse_replay_for_match(match_id, player0, player1):
  for player_id in [player0["id"], player1["id"]]:
    try:
      r = requests.get(
          f"https://aoe.ms/replay/?gameId={match_id}&profileId={player_id}")
    except Exception as e:
        continue
    if r.status_code == 404:
      continue
    # We found a valid map so unzip it
    try:
      replay_zip = zipfile.ZipFile(io.BytesIO(r.content))
      replay = replay_zip.read(replay_zip.namelist()[0])
      average_rating = (player0["elo"] + player1["elo"])/2.
      leaderboard_id = 3 #the rm 1v1 ladder
      parse_replays_and_store_in_db.parse_replay_file(
          match_id, player0["id"], player1["id"], average_rating,
          leaderboard_id, io.BytesIO(replay), VERSION)
      #Success!
      return

    except Exception as e:
      print(e)
      continue
  print(f'Could not download/parse {match_id}')
  return

# takes an array of player_ids and fetches all of their match histories
def get_match_history_for_player_ids(player_ids):
  try:
    matches = requests.get(
        f'https://aoe-api.reliclink.com/community/leaderboard/getRecentMatchHistory?title=age2&matchtype_id=8&profile_ids={player_ids}')
    if matches.status_code != 200:
      return None
  except Exception as e:
    print(e)
    return None
  return matches.json()

# takes an array of player ids to resolve
def get_elo_for_player_ids(player_ids):
  try:
    stats = requests.get(
        f'https://aoe-api.reliclink.com/community/leaderboard/getPersonalStat?title=age2&profile_ids={player_ids}')
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
    if stat["statgroup_id"] in player_stat_group_map:
      player_id = player_stat_group_map[stat["statgroup_id"]]
      if player_id not in player_id_dict:
        #new player and elo to add
        player_id_dict[player_id] = stat["rating"]
    else:
      print (f'invalid stat group found: {stat["statgroup_id"]}')
  return player_id_dict

def get_leaderboard():
  try:
    leaderboard = requests.get(
        'https://aoe-api.reliclink.com/community/leaderboard/getLeaderBoard2?leaderboard_id=3&title=age2')
    print(leaderboard.url)
    print(leaderboard.status_code)
    if leaderboard.status_code != 200:
      return None
  except Exception as e:
    print(e)
    return None
  return leaderboard.json()

def execute():
  player_id_dict = {} # connects player_id to most recently observed elo
  player_id_queue = queue.Queue()
  # allows for bulk resolving of player elo
  player_processing_queue = queue.Queue() # match_id, player_id, player_id
  added_matches_set = set() #set of added matches for quick lookups so we dont collide

  # Get starting player_id_set from top 200 on the aoe.ms api
  leaderboard = get_leaderboard()
  if leaderboard is None:
    return

  player_stat_group_map = {}
  for stat_group in leaderboard['statGroups']:
    for player in stat_group['members']:
      if player['profile_id'] not in player_stat_group_map:
        player_stat_group_map[player['personal_statgroup_id']] = \
            player['profile_id']
  for stat in leaderboard["leaderboardStats"]:
    if stat["statgroup_id"] in player_stat_group_map:
      player_id = player_stat_group_map[stat["statgroup_id"]]
      if player_id not in player_id_dict:
        #new player and elo to add
        player_id_dict[player_id] = stat["rating"]
        player_id_queue.put(player_id)
    else:
      print (f'invalid stat group found: {stat["statgroup_id"]}')
  print (len(player_id_dict))

  # Now that we have players, lets move through the queue and find matches!
  while not player_id_queue.empty():
    player_ids = [player_id_queue.get()];
    for i in range(PLAYERS_PER_QUERY-1):
      # do multiple players at once to reduce query volume
      if player_id_queue.empty():
        break
      player_ids.append(player_id_queue.get());
    matches = get_match_history_for_player_ids(player_ids)
    if matches == None:
      continue
    for match in matches["matchHistoryStats"]:
      match_id = match["id"]
      # ranked 1v1 only seems to be id 6
      if match["matchtype_id"] != 6:
        continue
      # sanity check that it is ranked
      if match["description"] != "AUTOMATCH":
        continue
      # make sure the match has exactly 2 players and ids!
      if len(match["matchhistoryreportresults"]) != 2:
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
      player0 = match["matchhistoryreportresults"][0]["profile_id"]
      player1 = match["matchhistoryreportresults"][1]["profile_id"]

      #add players to set and queue if not already found
      if player0 not in player_id_dict:
        player_id_dict[player0] = -1 # to be updated later
        player_id_queue.put(player0)
      if player1 not in player_id_dict:
        player_id_dict[player1] = -1 # to be updated
        player_id_queue.put(player1)
      #make sure match isnt already in db
      if parse_replays_and_store_in_db.does_match_exist(match_id):
        continue
      # make sure we havent already processed this match
      if match_id in added_matches_set:
        continue

      player_processing_queue.put((match_id, player0, player1))
      added_matches_set.add(match_id)

    # now process the player_processing_queue and pop matches with all players into the matches_resolve queue
    players_to_resolve = []
    matches_to_resolve = queue.Queue() # (match_id, player0_id, player1_id)
    while not player_processing_queue.empty():
      item = player_processing_queue.get()
      match_id = item[0]
      player0 = item[1]
      player1 = item[2]
      if player_id_dict[player0] >= 0 and player_id_dict[player1] >= 0:
        # both players exist so just fire off the match
        player_0_dict = {"id":player0, "elo":player_id_dict[player0]}
        player_1_dict = {"id":player1, "elo":player_id_dict[player1]}
        match_queue.put((match_id, player_0_dict, player_1_dict))
        continue
      if player_id_dict[player1] < 0:
        if player0 not in players_to_resolve:
          players_to_resolve.append(player0)
      if player_id_dict[player1] < 0:
        if player1 not in players_to_resolve:
          players_to_resolve.append(player1)
      # we have a match needing resolution so store it in the resolve queue
      matches_to_resolve.put(item)

      # now if there are enough players in the players to resolve array, resolve them or the queue is empty
      if len(players_to_resolve) >= ELOS_PER_QUERY or player_processing_queue.empty():
        player_elos = get_elo_for_player_ids(players_to_resolve)
        if player_elos is not None:
          for player, elo in player_elos.items():
            player_id_dict[player] = elo
        players_to_resolve.clear() # clear the queue
        # now fire off matches to resolve
        while not matches_to_resolve.empty():
          item = matches_to_resolve.get()
          match_id = item[0]
          player0 = item[1]
          player1 = item[2]
          # double check elos have been updated, else toss the match
          if player_id_dict[player0] < 0 or player_id_dict[player1] < 0:
            continue
          #send the relevant data to the queue!
          player_0_dict = {"id":player0, "elo":player_id_dict[player0]}
          player_1_dict = {"id":player1, "elo":player_id_dict[player1]}
          match_queue.put((match_id, player_0_dict, player_1_dict))

    print(player_id_queue.qsize(),len(player_id_dict))
    print(match_queue.qsize())
    # sleep a bit to not flood api
    time.sleep(1)
    while match_queue.qsize() > 100:
      #sleep is queue is saturated so we dont hit the api too much
      time.sleep(10)

def close():
  for process in processes:
    process.kill()
  exit()

if __name__ == "__main__":
  # first lets fire off the consumers so we can parse replays as we get them
  for i in range(NUM_PROCESSES):
    p = multiprocessing.Process(target=match_queue_consumer, args=(match_queue,))
    p.start()
    processes.append(p)

  while True:
    execute()
    close()
    # sleep 5 minutes on failed request to not flood the server
    time.sleep(300)
