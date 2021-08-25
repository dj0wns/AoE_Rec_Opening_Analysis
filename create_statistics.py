import os
import json

from parse_replays_and_store_in_db import connect_and_return

def opening_matchups(opening1, opening2):
  query = """SELECT
               sum(CASE WHEN a.victory = 1 OR a.victory = 0 THEN 1 ELSE 0 END) as Total,
               sum(CASE WHEN a.victory = 1 THEN 1 ELSE 0 END) AS FirstOpeningWins,
               sum(CASE WHEN b.victory = 1 THEN 1 ELSE 0 END) AS SecondOpeningWins,
               sum(CASE WHEN a.victory = -1 THEN 1 ELSE 0 END) AS Unknown
             FROM matches m
             JOIN match_players a ON a.match_id = m.id
             JOIN match_players b ON b.match_id = m.id
             WHERE a.opening_id = ? AND b.opening_id = ?
               AND a.id != b.id;"""
  args = (opening1, opening2, )
  return connect_and_return(query, args)[0]

def mirror_matchups(opening1):
  query = """SELECT COUNT(a.id)
             FROM matches m
             JOIN match_players a ON a.match_id = m.id
             JOIN match_players b ON b.match_id = m.id
             WHERE a.opening_id = ? AND b.opening_id = ?
               AND a.id != b.id
               AND a.victory = 1;"""
  args = (opening1, opening1, )
  return connect_and_return(query, args)[0]
 
def total_concluded_matches():
  query = """SELECT COUNT(a.id)
             FROM match_players a
             WHERE a.victory = 1;"""
  return connect_and_return(query, ())[0][0]
  

def get_strategies():
  query = """SELECT id, name from openings;"""
  return connect_and_return(query, ())

def get_civilizations():
  query = """SELECT DISTINCT civilization
             FROM match_players;"""
  return connect_and_return(query, ())

def get_civilization_count(civ_id):
  query = """SELECT 
               sum(CASE WHEN victory = 1 OR victory = 0 THEN 1 ELSE 0 END) as Total,
               sum(CASE WHEN victory = 1 THEN 1 ELSE 0 END) as Wins,
               sum(CASE WHEN victory = 0 THEN 1 ELSE 0 END) as Losses
             FROM match_players
             WHERE civilization = ?
             AND (victory = 1
             OR victory = 0);"""
  return connect_and_return(query, (civ_id,))[0]

if __name__ == '__main__':
  strategies = get_strategies()
  total_matches = total_concluded_matches()

  # Go through matchups
  print('Opening Stats!')
  for i in range(len(strategies)):
    for j in range(i, len(strategies)):
      if i == j:
        total = mirror_matchups(strategies[i][0])[0]
        if total:
          print(f'{strategies[i][1]} vs {strategies[j][1]} : {total} ({total/total_matches:.1%})')

      else:
        total, firstwins, secondwins, unknown = opening_matchups(strategies[i][0],strategies[j][0])
        if total:
          print(f'{strategies[i][1]} vs {strategies[j][1]}: {total} ({total/total_matches:.1%}), {firstwins}:{secondwins} ({firstwins/total:.1%}:{secondwins/total:.1%}) with {unknown} unknowns')
  
  #Go through civilizations
  print('\nCivilization Stats!')
  civilizations = get_civilizations()
  with open(os.path.join('aoe2techtree', 'data', 'data.json')) as json_file:
   aoe_data = json.load(json_file)

  #build civ dict
  civs = {}
  for name,value in aoe_data["civ_names"].items():
    civs[int(value)-10270] = name

  for i in range(len(civilizations)):
    total, wins, losses = get_civilization_count(civilizations[i][0])
    #divide play rate by 2 because there are 2 civs chosen for every match!
    print(f'{civs[civilizations[i][0]]} - {total} ({total/total_matches/2.:.1%}), {wins}:{losses} ({wins/total:.1%})')
