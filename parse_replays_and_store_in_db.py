import sqlite3
import aoe_replay_stats
import grab_replays_for_player
import os
import re
import sys

fpath=os.path.realpath(__file__)
path=os.path.dirname(fpath)
DB_FILE=path+"/local.db"

def init_db():
  sql_commands = []
  sql_commands.append("PRAGMA foreign_keys = ON;")
  sql_commands.append(""" CREATE TABLE IF NOT EXISTS openings (
                            id integer NOT NULL PRIMARY KEY,
                            name text NOT NULL
                            ) WITHOUT ROWID; """)
  
  for opening in aoe_replay_stats.OpeningType:
    sql_commands.append(f'INSERT OR IGNORE INTO openings (id, name) VALUES({opening.value}, "{opening.name}")')
  
  sql_commands.append(""" CREATE TABLE IF NOT EXISTS matches (
                            id integer NOT NULL PRIMARY KEY,
                            average_elo integer NOT NULL,
                            map_id integer NOT NULL,
                            time datetime DEFAULT CURRENT_TIMESTAMP
                            ) WITHOUT ROWID; """)

  sql_commands.append(""" CREATE TABLE IF NOT EXISTS players (
                            id integer NOT NULL PRIMARY KEY,
                            name text
                            ) WITHOUT ROWID; """)
  
  sql_commands.append(""" CREATE TABLE IF NOT EXISTS match_players (
                            id integer NOT NULL PRIMARY KEY,
                            player_id integer NOT NULL,
                            match_id integer NOT NULL,
                            opening_id integer DEFAULT -1,
                            civilization int DEFAULT -1,
                            victory int DEFAULT -1,
                            parser_version int DEFAULT 0,
                            time_parsed datetime DEFAULT CURRENT_TIMESTAMP,
                            CONSTRAINT fk_player FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
                            CONSTRAINT fk_match FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
                            UNIQUE(player_id, match_id)
                            ); """)
  
  sql_commands.append(""" CREATE TABLE IF NOT EXISTS match_player_actions (
                            id integer NOT NULL PRIMARY KEY,
                            match_player_id integer NOT NULL,
                            event_type integer NOT NULL,
                            event_id integer NOT NULL,
                            time integer NOT NULL,
                            duration integer NOT NULL,
                            CONSTRAINT fk_match_player_id FOREIGN KEY(match_player_id) REFERENCES match_players(id) ON DELETE CASCADE
                            ); """)
  try:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    for sql_command in sql_commands:
      c.execute(sql_command)
    conn.commit()
  except Exception as e:
    print(e)
  finally:
    conn.close()
 
### UNIVERSAL SQL FUNCTIONS ###
def connect_and_modify(statement):
  try:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(statement)
    conn.commit()
  except Exception as e:
    print(e)
  finally:
    conn.close()

def connect_and_modify(statement, args):
  try:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute(statement, args)
    conn.commit()
  except Exception as e:
    print(e)
  finally:
    conn.close()

def connect_and_modify_with_generator(generator):
  try:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    for statement, args in generator:
      c.execute(statement, args)
    conn.commit()
  except Exception as e:
    print(e)
  finally:
    conn.close()

def connect_and_return(statement, args):
  try:
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    if args is None:
      c.execute(statement)
    else:
      c.execute(statement, args)
    return c.fetchall()
  except Exception as e:
    print(e)
  finally:
    conn.close()

def add_player(player_id):
  connect_and_modify("INSERT OR IGNORE INTO players(id) VALUES(?)", (player_id,))

def add_match(match_id, average_elo, map_id):
  connect_and_modify("INSERT OR IGNORE INTO matches(id, average_elo, map_id) VALUES(?,?,?)", (match_id, average_elo, map_id))

def update_match_player(opening_id, match_player_id):
  connect_and_modify("""UPDATE match_players
                        SET opening_id = ?, parser_version = ?, time_parsed = CURRENT_TIMESTAMP
                        WHERE id = ?""", (opening_id, aoe_replay_stats.PARSER_VERSION, match_player_id))

def add_unparsed_match_player(player_id, match_id, civilization, victory):
  connect_and_modify("""INSERT OR IGNORE INTO match_players(player_id, match_id, civilization, victory) VALUES
                            (?,?,?,?)""", (player_id, match_id, civilization, victory))

def match_player_actions_generator(match_player_id, action_list):
  statement = """INSERT OR IGNORE INTO match_player_actions
                   (match_player_id, event_type, event_id, time, duration)
                   VALUES (?,?,?,?,?)"""
  for unique_action in action_list:
    yield statement, (match_player_id, unique_action.event_type.value, unique_action.id, unique_action.timestamp, unique_action.duration)

def add_match_player_actions(match_player_id, action_list):
  generator = match_player_actions_generator(match_player_id, action_list)
  connect_and_modify_with_generator(generator)
    
def does_match_exist(match_id):
  match = connect_and_return("SELECT * FROM matches WHERE id=?", (match_id,))
  if len(match) == 0:
    return False
  return True

def get_match_player_id(player_id, match_id):
  match_player_id = connect_and_return("SELECT id FROM match_players WHERE player_id = ? AND match_id = ?", (player_id, match_id))
  if len(match_player_id) == 0: 
    return None
  return match_player_id[0][0]

def get_match_players_needing_update():
  match_players = connect_and_return("SELECT * FROM match_players WHERE parser_version < ?", (aoe_replay_stats.PARSER_VERSION,))
  if len(match_players) == 0: 
    return None
  return match_players

def get_actions_for_match_player(match_player_id):
  match_player_actions = connect_and_return("SELECT * FROM match_player_actions WHERE match_player_id = ?", (match_player_id,))
  if len(match_player_actions) == 0: 
    return None
  return match_player_actions
  

if __name__ == '__main__':
  init_db()
  #import a folder of replays, first add replays to db and then do analysis after
  for file in os.listdir(sys.argv[1]):
    file = os.path.join(sys.argv[1], file)
    #important info in the map name
    match_id, player1_id, player2_id, average_elo = grab_replays_for_player.parse_filename(file)
    #match already in db!
    if does_match_exist(match_id):
      continue
    try :
      players, header, civs, loser_id = aoe_replay_stats.parse_replay(file)
    except Exception as e:
      print(e)
      continue
    #now plug it into the db!
    #first add players
    player_ids = [player1_id, player2_id]
    player_num = 0

    add_player(player1_id)
    add_player(player2_id)
    add_match(match_id, average_elo, header.de.selected_map_id)
    
    for i in range(len(players)):
      if not players[i]:
        continue
      # now add match player
      winner_value = -1
      if loser_id is not None:
        if loser_id == i:
          winner_value = 0
        else:
          winner_value = 1
      #first add template match_player with basic info
      add_unparsed_match_player(player_ids[player_num], match_id, header.de.players[player_num].civ_id, winner_value)
      match_player_id = get_match_player_id(player_ids[player_num], match_id)
      #now add player actions
      add_match_player_actions(match_player_id, players[i])
      player_num += 1

  #now do analytics
  for match_player in get_match_players_needing_update():
    #treat players opener regardless of opponent for this stage
    player_event_list = []
    events = []
    for action in get_actions_for_match_player(match_player[0]):
      events.append(aoe_replay_stats.Event(aoe_replay_stats.EventType(action[2]), action[3], None, action[4], action[5]))
    player_event_list.append(events)
    player_strategies = aoe_replay_stats.guess_strategy(player_event_list)
    aoe_replay_stats.print_events(player_event_list, None, None, player_strategies)
    
    #now just update match_player
    update_match_player(player_strategies[0].value, match_player[0])
