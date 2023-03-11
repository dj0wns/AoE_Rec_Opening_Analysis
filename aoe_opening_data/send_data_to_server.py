import os
import json
import requests
import sqlite3
import time
import re
from datetime import datetime;

from dotenv import load_dotenv

import aoe_replay_stats
import parse_replays_and_store_in_db

load_dotenv()

API_KEY = os.getenv('API_KEY')

GET_ENDPOINT = 'https://www.aoepulse.com/api/v1/last_uploaded_match/'
POST_ENDPOINT = 'https://www.aoepulse.com/api/v1/import_matches/'

#API_KEY = os.getenv('LOCAL_API_KEY')
#
#GET_ENDPOINT = 'http://127.0.0.1:8000/api/v1/last_uploaded_match/'
#POST_ENDPOINT = 'http://127.0.0.1:8000/api/v1/import_matches/'

def match_to_dict(match):
  # This must match query statement in "send_matches_to_server"
  output_dict = {}
  output_dict['id'] = match[0]
  #fix error in db where some elos were inserted as strings due to weird replay naming
  if type(match[1]) == str:
    elo = int(re.search('^\d+',str(match[1]))[0])
  else:
    elo = match[1]
  output_dict['average_elo'] = round(elo) #make sure elo is an integer
  output_dict['map_id'] = match[2]
  output_dict['time'] = match[3]
  output_dict['patch_id'] = match[4]
  output_dict['ladder_id'] = match[5]
  #remove any none patches that seem to come during updated
  output_dict['patch_number'] = match[6] if match[6] is not None else -1
  output_dict['player1'] = match[7]
  for i in range(32):
    output_dict[f'player1_opening_flag{i}'] = (match[8] & 2**i) != 0

  output_dict['player1_civilization'] = match[9]
  output_dict['player1_victory'] = match[10]
  output_dict['player1_parser_version'] = aoe_replay_stats.PARSER_VERSION

  output_dict['player2'] = match[11]
  for i in range(32):
    output_dict[f'player2_opening_flag{i}'] = (match[12] & 2**i) != 0

  output_dict['player2_civilization'] = match[13]
  output_dict['player2_victory'] = match[14]
  output_dict['player2_parser_version'] = aoe_replay_stats.PARSER_VERSION
  return output_dict

def match_player_action_to_dict(action, match_id, player_id):
  # This must match query statement in "send_matches_to_server"
  output_dict = {}
  output_dict['match'] = match_id
  output_dict['player'] = player_id
  output_dict['event_type'] = action[0]
  output_dict['event_id'] = action[1]
  output_dict['time'] = action[2]
  output_dict['duration'] = action[3]

  return output_dict

def send_matches_to_server():
  r = requests.get(GET_ENDPOINT)
  last_match_time = r.json()['time']
  last_match_datetime = datetime.strptime(last_match_time, '%Y-%m-%dT%H:%M:%S%z')
  print('last_match_time: ', last_match_time, type(last_match_time), last_match_datetime)
  conn = sqlite3.connect(parse_replays_and_store_in_db.DB_FILE)
  cursor = conn.cursor()
  start = time.time()
  # get matches that are not currently on the server
  cursor.execute("""select id, time from matches order by id DESC limit 1;""")
  matches = cursor.fetchone()
  print(matches[0], matches[1], type(matches[1]))
  cursor.execute(
    """SELECT m.id, m.average_elo, m.map_id, m.time, m.patch_id, m.ladder_id, m.patch_number,
              a.player_id, a.opening_id, a.civilization, a.victory,
              b.player_id, b.opening_id, b.civilization, b.victory,
              a.id, b.id
                        from matches m
                        JOIN match_players a on a.match_id = m.id
                        JOIN match_players b on b.match_id = m.id
                        WHERE a.id != b.id
                          AND a.parser_version == ?
                          AND b.parser_version == ?
                          AND a.victory == 1
                          AND m.time > ?
                          AND (m.ladder_id == 3 OR m.ladder_id == 13)
                          ORDER BY m.time
                          LIMIT 100000000
                          """,
    (aoe_replay_stats.PARSER_VERSION, aoe_replay_stats.PARSER_VERSION, last_match_datetime))
  matches = cursor.fetchall()
  end = time.time()
  print(f'Query took {end - start} seconds.')

  # go through pending matches in slices to prevent more expensive db queries
  NUM_TO_UPLOAD = 500
  print(f'Matches to upload: {len(matches)}')
  list_of_slices = zip(*(iter(matches),) * NUM_TO_UPLOAD)
  number_of_slices = len(matches) / NUM_TO_UPLOAD
  count = 0
  for match_set in list_of_slices:
    print(f'Sending set number {count} / {number_of_slices}')
    post_dict = {'matches':[], 'match_player_actions':[], 'players':set(), 'patches':set()}
    for match in match_set:
      #update bad values of patch
      #from those match ids get the corresponding match player actions for player1
      post_dict['matches'].append(match_to_dict(match))
      # add players to dict
      post_dict['players'].add(match[7])
      post_dict['players'].add(match[11])
      #add patch
      #remove any none patches that seem to come during updated
      post_dict['patches'].add(match[6] if match[6] is not None else -1)

    #dedupe match_player_actions - lazy, should just enforce uniqueness in main db but here we are
    post_dict['match_player_actions'] = [i for n, i in enumerate(post_dict['match_player_actions']) if i not in post_dict['match_player_actions'][n + 1:]]
    #convert sets back to lists
    post_dict['players'] = [ {'id':i} for i in post_dict['players']]
    post_dict['patches'] = [ {'id':i} for i in post_dict['patches']]

    #now send it and see what happens!
    end = time.time()
    print(f'Building request took {end - start} seconds.')
    r = requests.post(POST_ENDPOINT, json=post_dict, headers={'Authorization': 'Api-Key '+API_KEY})
    end = time.time()
    print(f'Full loop toop {end - start} seconds.')
    print(f'Sent set number {count} / {number_of_slices}')
    count += 1
    print('sleeping 1s')
    time.sleep(1)
  conn.close()

if __name__ == '__main__':
  #do stuff
  send_matches_to_server()
