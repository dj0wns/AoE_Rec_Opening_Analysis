import os
import json
import argparse

from parse_replays_and_store_in_db import connect_and_return
from aoe_replay_stats import output_time

aoe_data = None

def arguments_to_query_string(match_table_tag,
                              match_playera_table_tag,
                              match_playerb_table_tag,
                              minimum_elo,
                              maximum_elo,
                              map_ids,
                              include_civ_ids,
                              clamp_civ_ids,
                              no_mirror,
                              exclude_civ_ids,
                              include_ladder_ids,
                              include_patch_ids,
                              clamp_player1,
                              player_ids):
  string = "("
  string += f'{match_table_tag}.average_elo > {minimum_elo}\n'
  string += f'  AND {match_table_tag}.average_elo < {maximum_elo}\n'

  if no_mirror:
    string += f'  AND {match_playera_table_tag}.civilization != {match_playerb_table_tag}.civilization\n'

  if map_ids is not None:
    string += '  AND ('
    for i in range(len(map_ids)):
      for j in range(len(map_ids[i])):
        if i+j > 0:
          string +='    OR '
        string += f'{match_table_tag}.map_id = {map_ids[i][j]}\n'
    string += '       )\n'

  if include_civ_ids:
    string += '  AND ('
    for i in range(len(include_civ_ids)):
      if i > 0:
        string +='    OR '
      string += f'{match_playera_table_tag}.civilization = {include_civ_ids[i]}\n'
      if not clamp_player1:
        string += f'    OR {match_playerb_table_tag}.civilization = {include_civ_ids[i]}\n'
    string += '       )\n'

  if clamp_civ_ids:
    string += '  AND ('
    for i in range(len(clamp_civ_ids)):
      if i > 0:
        string +='    OR '
      string += f'{match_playera_table_tag}.civilization = {clamp_civ_ids[i]}\n'
    string += '       )\n  AND ('
    for i in range(len(clamp_civ_ids)):
      if i > 0:
        string +='    OR '
      string += f'{match_playerb_table_tag}.civilization = {clamp_civ_ids[i]}\n'
    string += '       )\n'
  string += ")"

  if exclude_civ_ids:
    string += '  AND ('
    for i in range(len(exclude_civ_ids)):
      if i > 0:
        string +='    AND '
      string += f'{match_playera_table_tag}.civilization != {exclude_civ_ids[i]}\n'
      string += f'    AND {match_playerb_table_tag}.civilization != {exclude_civ_ids[i]}\n'
    string += '       )\n'

  if include_ladder_ids is not None:
    string += '  AND ('
    for i in range(len(include_ladder_ids)):
      for j in range(len(include_ladder_ids[i])):
        if i+j > 0:
          string +='    OR '
        string += f'{match_table_tag}.ladder_id = {include_ladder_ids[i][j]}\n'
    string += '       )\n'

  if include_patch_ids is not None:
    string += '  AND ('
    for i in range(len(include_patch_ids)):
      for j in range(len(include_patch_ids[i])):
        if i+j > 0:
          string +='    OR '
        string += f'{match_table_tag}.patch_id = {include_patch_ids[i][j]}\n'
    string += '       )\n'
  
  if player_ids is not None:
    string += '  AND ('
    for i in range(len(player_ids)):
      for j in range(len(player_ids[i])):
        if i+j > 0:
          string +='    OR '
        string += f'{match_playera_table_tag}.player_id = {player_ids[i][j]}\n'
        if not clamp_player1:
         string += f'    OR {match_playerb_table_tag}.player_id = {player_ids[i][j]}\n'
    string += '       )\n'

  return string

def opening_matchups(opening1, opening2, minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids):
  query = """SELECT
               sum(CASE WHEN a.victory = 1 OR a.victory = 0 THEN 1 ELSE 0 END) as Total,
               sum(CASE WHEN a.victory = 1 THEN 1 ELSE 0 END) AS FirstOpeningWins,
               sum(CASE WHEN b.victory = 1 THEN 1 ELSE 0 END) AS SecondOpeningWins,
               sum(CASE WHEN a.victory = -1 THEN 1 ELSE 0 END) AS Unknown
             FROM matches m
             JOIN match_players a ON a.match_id = m.id
             JOIN match_players b ON b.match_id = m.id
             WHERE a.opening_id = ? AND b.opening_id = ?
               AND a.id != b.id
               AND """
  query += arguments_to_query_string('m',
                                     'a',
                                     'b',
                                     minimum_elo,
                                     maximum_elo,
                                     map_ids,
                                     include_civ_ids,
                                     clamp_civ_ids,
                                     no_mirror,
                                     exclude_civ_ids,
                                     include_ladder_ids,
                                     include_patch_ids,
                                     include_civ_ids or player_ids is not None,
                                     player_ids)
  query += ';'
  args = (opening1, opening2, )
  return connect_and_return(query, args)[0]

def mirror_matchups(opening1, minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids):
  query = """SELECT COUNT(a.id)
             FROM matches m
             JOIN match_players a ON a.match_id = m.id
             JOIN match_players b ON b.match_id = m.id
             WHERE a.opening_id = ? AND b.opening_id = ?
               AND a.id != b.id
               AND a.victory = 1
               AND"""
  query += arguments_to_query_string('m',
                                     'a',
                                     'b',
                                     minimum_elo,
                                     maximum_elo,
                                     map_ids,
                                     include_civ_ids,
                                     clamp_civ_ids,
                                     no_mirror,
                                     exclude_civ_ids,
                                     include_ladder_ids,
                                     include_patch_ids,
                                     include_civ_ids or player_ids is not None,
                                     player_ids)
  query += ';'
  args = (opening1, opening1, )
  return connect_and_return(query, args)[0]

#Clamps to included civs!
def age_up_times_for_opening(opening1, minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids, tech_ids):
  query = """SELECT a.id, c.event_id, c.time, c.duration
             FROM matches m
             JOIN match_players a ON a.match_id = m.id
             JOIN match_players b ON b.match_id = m.id
             JOIN match_player_actions c ON a.id = c.match_player_id
             WHERE a.opening_id = ?
               AND a.id != b.id
               AND c.event_type = 3
               AND (c.event_id = 101
                 OR c.event_id = 102
                 OR c.event_id = 103"""
  if tech_ids is not None:
    query += '    OR '
    for i in range(len(tech_ids)):
      for j in range(len(tech_ids[i])):
        if i+j > 0:
          query +='    OR '
        query += f'c.event_id = {tech_ids[i][j]}\n'
  query += ") "
  query += " AND "
  query += arguments_to_query_string('m',
                                     'a',
                                     'b',
                                     minimum_elo,
                                     maximum_elo,
                                     map_ids,
                                     include_civ_ids,
                                     clamp_civ_ids,
                                     no_mirror,
                                     exclude_civ_ids,
                                     include_ladder_ids,
                                     include_patch_ids,
                                     True,
                                     player_ids)
  query += 'ORDER BY a.id;'
  args = (opening1, )
  return connect_and_return(query, args)

def total_concluded_matches(minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids):
  query = """SELECT COUNT(a.id)
             FROM match_players a
             JOIN matches m ON m.id = a.match_id
             join match_players b on a.match_id = b.match_id
             WHERE a.victory = 1
               AND a.id != b.id
               AND """
  query += arguments_to_query_string('m',
                                     'a',
                                     'b',
                                     minimum_elo,
                                     maximum_elo,
                                     map_ids,
                                     include_civ_ids,
                                     clamp_civ_ids,
                                     no_mirror,
                                     exclude_civ_ids,
                                     include_ladder_ids,
                                     include_patch_ids,
                                     False,
                                     player_ids)
  if no_mirror:
    query += """AND a.civilization != b.civilization"""
  query += ';'
  return connect_and_return(query, ())[0][0]


def get_strategies():
  query = """SELECT id, name from openings;"""
  return connect_and_return(query, ())

def get_civilizations():
  query = """SELECT DISTINCT civilization
             FROM match_players;"""
  return connect_and_return(query, ())

def get_civilization_count(civ_id, minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids):
  query = """SELECT
               sum(CASE WHEN a.victory = 1 OR a.victory = 0 THEN 1 ELSE 0 END) as Total,
               sum(CASE WHEN a.victory = 1 THEN 1 ELSE 0 END) as Wins,
               sum(CASE WHEN a.victory = 0 THEN 1 ELSE 0 END) as Losses
             FROM match_players a
             JOIN matches m ON m.id = a.match_id
             join match_players b on a.match_id = b.match_id
             WHERE a.civilization = ?
             AND a.civilization != b.civilization
             AND (a.victory = 1
             OR a.victory = 0)
             AND a.id != b.id
             AND"""

  query += arguments_to_query_string('m',
                                     'a',
                                     'b',
                                     minimum_elo,
                                     maximum_elo,
                                     map_ids,
                                     include_civ_ids,
                                     clamp_civ_ids,
                                     False,
                                     exclude_civ_ids,
                                     include_ladder_ids,
                                     include_patch_ids,
                                     player_ids is not None,
                                     player_ids)
  query += ';'
  return connect_and_return(query, (civ_id,))[0]

def execute(minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids, tech_ids):
  strategies = get_strategies()
  total_matches = total_concluded_matches(minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids)


  if not total_matches:
    print ("No matches found matching the criteria")
    return

  # Go through matchups
  print('Uptimes and Selected technologies!')
  #average times through the ages
  translated_names = {
    101:"Feudal Age",
    102:"Castle Age"}
  for i in range(len(strategies)):
    age_up_times = age_up_times_for_opening(strategies[i][0], minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids, tech_ids)
    if not age_up_times:
      continue
    research_dict = {}

    for event in age_up_times:
      if event[1] in research_dict:
        research_dict[event[1]]["time"] += event[2]
        research_dict[event[1]]["count"] += 1
      else:
        research_dict[event[1]] = {"time" : event[2], "count" : 1}
    string = f'{strategies[i][1]}: '
    for k, v in sorted(research_dict.items()):
      if k in translated_names:
        string += translated_names[k] + ": "
      else: 
        string += aoe_data["data"]["techs"][str(k)]["internal_name"] + ": "
      string += output_time(v["time"]/v["count"] + int(aoe_data["data"]["techs"][str(k)]["ResearchTime"])*1000)
      string += ", "
    print (string)

  print("\nStrategy Matchups!")
  for i in range(len(strategies)):
    if include_civ_ids or player_ids is not None:
      iteration_range = range(len(strategies))
    else:
      iteration_range = range(i, len(strategies))
    for j in iteration_range:
      if i == j:
        if include_civ_ids or player_ids is not None:
          total, firstwins, secondwins, unknown = opening_matchups(strategies[i][0],strategies[j][0], minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids)
          if total:
            print(f'{strategies[i][1]} vs {strategies[j][1]} - {total} ({total/total_matches:.1%}), {firstwins}:{secondwins} ({firstwins/total:.1%}:{secondwins/total:.1%}) with {unknown} unknowns')
        else:
          total = mirror_matchups(strategies[i][0], minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids)[0]
          if total:
            print(f'{strategies[i][1]} vs {strategies[j][1]} - {total} ({total/total_matches:.1%})')

      else:
        total, firstwins, secondwins, unknown = opening_matchups(strategies[i][0],strategies[j][0], minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids)
        if total:
          print(f'{strategies[i][1]} vs {strategies[j][1]} - {total} ({total/total_matches:.1%}), {firstwins}:{secondwins} ({firstwins/total:.1%}:{secondwins/total:.1%}) with {unknown} unknowns')

  #Go through civilizations
  print('\nCivilization Stats!')
  civilizations = get_civilizations()

  #build civ dict
  civs = {}
  for name,value in aoe_data["civ_names"].items():
    civs[int(value)-10270] = name

  total_matches = total_concluded_matches(minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, True, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids)
  for i in range(len(civilizations)):
    total, wins, losses = get_civilization_count(civilizations[i][0], minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, exclude_civ_ids, include_ladder_ids, include_patch_ids, player_ids)
    if total:
      #divide play rate by 2 because there are 2 civs chosen for every match!
      print(f'{civs[civilizations[i][0]]} - {total} ({total/total_matches/2.:.1%}), {wins}:{losses} ({wins/total:.1%})')

def names_to_ids(include_civ_ids, clamp_civ_ids, exclude_civ_ids):
  new_include_civ_ids = []
  new_clamp_civ_ids = []
  new_exclude_civ_ids = []
  if include_civ_ids is not None:
    for i in range(len(include_civ_ids)):
      for j in range(len(include_civ_ids[i])):
        if include_civ_ids[i][j] in aoe_data["civ_names"]:
          new_include_civ_ids.append(int(aoe_data["civ_names"][include_civ_ids[i][j]]) - 10270)
        else:
          print(f'{include_civ_ids[i][j]} is not a valid civ name!')
          exit(0)
  if clamp_civ_ids is not None:
    for i in range(len(clamp_civ_ids)):
      for j in range(len(clamp_civ_ids[i])):
        if clamp_civ_ids[i][j] in aoe_data["civ_names"]:
          new_clamp_civ_ids.append(int(aoe_data["civ_names"][clamp_civ_ids[i][j]]) - 10270)
        else:
          print(f'{clamp_civ_ids[i][j]} is not a valid civ name!')
          exit(0)
  if exclude_civ_ids is not None:
    for i in range(len(exclude_civ_ids)):
      for j in range(len(exclude_civ_ids[i])):
        if exclude_civ_ids[i][j] in aoe_data["civ_names"]:
          new_exclude_civ_ids.append(int(aoe_data["civ_names"][exclude_civ_ids[i][j]]) - 10270)
        else:
          print(f'{exclude_civ_ids[i][j]} is not a valid civ name!')
          exit(0)
  return new_include_civ_ids, new_clamp_civ_ids, new_exclude_civ_ids

if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Build tailored statistics from the replay database")
  parser.add_argument("-e", "--minimum-elo", help="Minimum match elo for all results", type=int, default=0)
  parser.add_argument("-E", "--maximum-elo", help="Maximum match elo for all results", type=int, default=9999)
  parser.add_argument("-l", "--include-ladder-ids", help="Only include games played on these ladders (3=1v1, 13=EW)", type=int, action='append', nargs='+')
  parser.add_argument("-p", "--include-patch-ids", help="Only include games played on these patches (current as of writing this is 25.01)", type=str, action='append', nargs='+')
  parser.add_argument("-m", "--map-ids", help="Restrict all results to these map ids", type=int, action='append', nargs='+')
  parser.add_argument("-c", "--include-civ-names", help="Include any matches with at least 1 of these civs", type=str, action='append', nargs='+')
  parser.add_argument("-C", "--clamp-civ-names", help="Only include games where matches only have civs in this pool", type=str, action='append', nargs='+')
  parser.add_argument("-x", "--exclude-civ-names", help="Remove games where these civs are present", type=str, action='append', nargs='+')
  parser.add_argument("-i", "--player-ids", help="Restrict all results to games with these player ids", type=int, action='append', nargs='+')
  parser.add_argument("-t", "--tech-ids", help="Include these tech ids in the average research time section", type=int, action='append', nargs='+')
  parser.add_argument("-n", "--no-mirror", help="Remove games where there are mirror matches", action='store_true')
  args = parser.parse_args()

  with open(os.path.join('aoe2techtree', 'data', 'data.json')) as json_file:
   aoe_data = json.load(json_file)
  include_civ_ids, clamp_civ_ids, exclude_civ_ids = names_to_ids(args.include_civ_names, args.clamp_civ_names, args.exclude_civ_names)
  execute(args.minimum_elo, args.maximum_elo, args.map_ids, include_civ_ids, clamp_civ_ids, args.no_mirror, exclude_civ_ids, args.include_ladder_ids, args.include_patch_ids, args.player_ids, args.tech_ids)
