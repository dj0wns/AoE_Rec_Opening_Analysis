import os
import json
import argparse

from parse_replays_and_store_in_db import connect_and_return
from aoe_replay_stats import output_time

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
                              include_patch_ids):
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

  if include_civ_ids is not None:
    string += '  AND ('
    for i in range(len(include_civ_ids)):
      for j in range(len(include_civ_ids[i])):
        if i+j > 0:
          string +='    OR '
        string += f'{match_playera_table_tag}.civilization = {include_civ_ids[i][j]}\n'
        string += f'    OR {match_playerb_table_tag}.civilization = {include_civ_ids[i][j]}\n'
    string += '       )\n'

  if clamp_civ_ids is not None:
    string += '  AND ('
    for i in range(len(clamp_civ_ids)):
      for j in range(len(clamp_civ_ids[i])):
        if i+j > 0:
          string +='    OR '
        string += f'{match_playera_table_tag}.civilization = {clamp_civ_ids[i][j]}\n'
    string += '       )\n  AND ('
    for i in range(len(clamp_civ_ids)):
      for j in range(len(clamp_civ_ids[i])):
        if i+j > 0:
          string +='    OR '
        string += f'{match_playerb_table_tag}.civilization = {clamp_civ_ids[i][j]}\n'
    string += '       )\n'
  string += ")"

  if exclude_civ_ids is not None:
    string += '  AND ('
    for i in range(len(exclude_civ_ids)):
      for j in range(len(exclude_civ_ids[i])):
        if i+j > 0:
          string +='    AND '
        string += f'{match_playera_table_tag}.civilization != {exclude_civ_ids[i][j]}\n'
        string += f'    AND {match_playerb_table_tag}.civilization != {exclude_civ_ids[i][j]}\n'
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

  return string

def opening_matchups(opening1, opening2, minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids):
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
                                     include_patch_ids)
  query += ';'
  args = (opening1, opening2, )
  return connect_and_return(query, args)[0]

def mirror_matchups(opening1, minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids):
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
                                     include_patch_ids)
  query += ';'
  args = (opening1, opening1, )
  return connect_and_return(query, args)[0]

def age_up_times_for_opening(opening1, minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids):
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
                 OR c.event_id = 103)
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
                                     include_patch_ids)
  query += 'ORDER BY a.id;'
  args = (opening1, )
  return connect_and_return(query, args)

def total_concluded_matches(minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids):
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
                                     include_patch_ids)
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

def get_civilization_count(civ_id, minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, exclude_civ_ids, include_ladder_ids, include_patch_ids):
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
                                     include_patch_ids)
  query += ';'
  return connect_and_return(query, (civ_id,))[0]

def execute(minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids):
  strategies = get_strategies()
  total_matches = total_concluded_matches(minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids)

  with open(os.path.join('aoe2techtree', 'data', 'data.json')) as json_file:
   aoe_data = json.load(json_file)

  if not total_matches:
    print ("No matches found matching the criteria")
    return

  # Go through matchups
  print('Opening Stats!')
  #average times through the ages
  for i in range(len(strategies)):
    feudal = 0
    feudal_count = 0
    castle = 0
    castle_count = 0
    imperial = 0
    imperial_count = 0
    age_up_times = age_up_times_for_opening(strategies[i][0], minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids)
    if not age_up_times:
      continue
    for event in age_up_times:
      if event[1] == 101: #feudal age
        feudal += event[2]
        feudal_count += 1
      elif event[1] == 102: #castle age
        castle += event[2]
        castle_count += 1
      elif event[1] == 103: #imperial age
        imperial += event[2]
        imperial_count += 1
    if not feudal:
      feudal_count = 1
    if not castle:
      castle_count = 1
    if not imperial:
      imperial_count = 1
    feudal_time = output_time(feudal/feudal_count + int(aoe_data["data"]["techs"][str(101)]["ResearchTime"])*1000)
    castle_time = output_time(castle/castle_count + int(aoe_data["data"]["techs"][str(102)]["ResearchTime"])*1000)
    imperial_time = output_time(imperial/imperial_count + int(aoe_data["data"]["techs"][str(103)]["ResearchTime"])*1000)
    print(f'{strategies[i][1]}: Feudal: {feudal_time}, Castle: {castle_time}, Imperial: {imperial_time}')

  print ("")
  for i in range(len(strategies)):
    for j in range(i, len(strategies)):
      if i == j:
        total = mirror_matchups(strategies[i][0], minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids)[0]
        if total:
          print(f'{strategies[i][1]} vs {strategies[j][1]} : {total} ({total/total_matches:.1%})')

      else:
        total, firstwins, secondwins, unknown = opening_matchups(strategies[i][0],strategies[j][0], minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids)
        if total:
          print(f'{strategies[i][1]} vs {strategies[j][1]}: {total} ({total/total_matches:.1%}), {firstwins}:{secondwins} ({firstwins/total:.1%}:{secondwins/total:.1%}) with {unknown} unknowns')

  #Go through civilizations
  print('\nCivilization Stats!')
  civilizations = get_civilizations()

  #build civ dict
  civs = {}
  for name,value in aoe_data["civ_names"].items():
    civs[int(value)-10270] = name

  total_matches = total_concluded_matches(minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, True, exclude_civ_ids, include_ladder_ids, include_patch_ids)
  for i in range(len(civilizations)):
    total, wins, losses = get_civilization_count(civilizations[i][0], minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids, exclude_civ_ids, include_ladder_ids, include_patch_ids)
    if total:
      #divide play rate by 2 because there are 2 civs chosen for every match!
      print(f'{civs[civilizations[i][0]]} - {total} ({total/total_matches/2.:.1%}), {wins}:{losses} ({wins/total:.1%})')


if __name__ == '__main__':
  parser = argparse.ArgumentParser(description="Build tailored statistics from the replay database")
  parser.add_argument("-e", "--minimum-elo", help="Minimum match elo for all results", type=int, default=0)
  parser.add_argument("-E", "--maximum-elo", help="Maximum match elo for all results", type=int, default=9999)
  parser.add_argument("-l", "--include-ladder-ids", help="Only include games played on these ladders (3=1v1, 13=EW)", type=int, action='append', nargs='+')
  parser.add_argument("-p", "--include-patch-ids", help="Only include games played on these patches (current as of writing this is 25.01)", type=str, action='append', nargs='+')
  parser.add_argument("-m", "--map-ids", help="Restrict all results to these map ids", type=int, action='append', nargs='+')
  parser.add_argument("-c", "--include-civ-ids", help="Include any matches with at least 1 of these civs", type=int, action='append', nargs='+')
  parser.add_argument("-C", "--clamp-civ-ids", help="Only include games where matches only have civs in this pool", type=int, action='append', nargs='+')
  parser.add_argument("-x", "--exclude-civ-ids", help="Remove games where these civs are present", type=int, action='append', nargs='+')
  parser.add_argument("-n", "--no-mirror", help="Remove games where there are mirror matches", action='store_true')
  args = parser.parse_args()

  execute(args.minimum_elo, args.maximum_elo, args.map_ids, args.include_civ_ids, args.clamp_civ_ids, args.no_mirror, args.exclude_civ_ids, args.include_ladder_ids, args.include_patch_ids)
