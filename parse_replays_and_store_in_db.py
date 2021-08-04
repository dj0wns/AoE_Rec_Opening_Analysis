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
                            map_id integer NOT NULL
                            ) WITHOUT ROWID; """)

  sql_commands.append(""" CREATE TABLE IF NOT EXISTS players (
                            id integer NOT NULL PRIMARY KEY,
                            name text
                            ) WITHOUT ROWID; """)
  
  sql_commands.append(""" CREATE TABLE IF NOT EXISTS match_players (
                            id integer NOT NULL PRIMARY KEY,
                            player_id integer NOT NULL,
                            match_id integer NOT NULL,
                            opening_id integer NOT_NULL,
                            civilization text NOT_NULL,
                            victory bool NOT NULL,
                            CONSTRAINT fk_player FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE,
                            CONSTRAINT fk_match FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
                            UNIQUE(player_id, match_id)
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

def add_match_player(player_id, match_id, opening_id, civilization, victory):
  connect_and_modify("""INSERT OR IGNORE INTO match_players(player_id, match_id, opening_id, civilization, victory) VALUES
                            (?,?,?,?,?)""", (player_id, match_id, opening_id, civilization, victory))
    
def does_match_exist(match_id):
  match = connect_and_return("SELECT * FROM matches WHERE id=?", (match_id,))
  if len(match) == 0:
    return False
  return True
  

if __name__ == '__main__':
  init_db()
  #import a folder of replays!
  for file in os.listdir(sys.argv[1]):
    file = os.path.join(sys.argv[1], file)
    #important info in the map name
    match_id, player1_id, player2_id, average_elo = grab_replays_for_player.parse_filename(file)
    #match already in db!
    if does_match_exist(match_id):
      continue
    try :
      players, header, civs = aoe_replay_stats.parse_replay(file)
    except Exception as e:
      print(e)
      continue
    player_strategies = aoe_replay_stats.guess_strategy(players, header, civs)
    if not player_strategies or len(player_strategies) < 2:
      continue
    if not player_strategies[0] or not player_strategies[1]:
      continue
    #add players to db
    add_player(player1_id)
    add_player(player2_id)
    add_match(match_id, average_elo, header.de.selected_map_id)
    #add each player manually because lazy
    add_match_player(player1_id, match_id, player_strategies[0].value, civs[header.de.players[0].civ_id], False)
    add_match_player(player2_id, match_id, player_strategies[1].value, civs[header.de.players[1].civ_id], False)


  
  
