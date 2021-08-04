import os
import sys
import math
import json
from mgz import header, fast, enums, const
from mgz.enums import OperationEnum
from construct import Byte
from collections import OrderedDict
from enum import Enum

class EventType(Enum):
  UNIT=1
  BUILDING=2
  TECH=3

class OpeningType(Enum):
  Unknown=0
  PremillDrush=1
  PremillDrushFlush=2
  PremillDrushFC=3
  PostmillDrush=4
  PostmillDrushFlush=5
  PostmillDrushFC=6
  Maa=16
  MaaArchers=7
  MaaScouts=8
  MaaCastle=9
  Scouts=10
  ScoutsArchers=11
  ScoutsSkirmishers=12
  StraightArchers=13
  StraightArchers1Range=14
  StraightArchers2Range=15
  FastCastle=16

# UNIT IDS #
UNIT_IDS = {
  "Archer":4,
  "Skirmisher": 7,
  "Knight": 38,
  "Cavalry Archer":39,
  "Militia":74,
  "MAA":75,
  "Villager":83,
  "Spearman":93,
  "Monk":125,
  "Scorpion":279,
  "Mangonel":280,
  "Camel":329,
  "Scout":448,
  "Eagle":751,
  "Battering Ram":1258,
}

#ONLY BUILDINGS WE CARE ABOUT
BUILDING_IDS = {
  "Archery Range": 10,
  "Archery Range": 87,
  "Barracks": 12,
  "Blacksmith 2":18,
  "Blacksmith 3":19,
  "Monastery 2": 30,
  "Monastery 3": 31,
  "Monastery 4": 32,
  "Siege Workshop": 49,
  "Dock": 51,
  "Mill": 68,
  "Watch Tower": 79,
  "Market": 84,
  "Stable 2": 86,
  "Stable": 101,
  "Blacksmith":103,
  "Monastery":104,
  "Town Center 2": 109,
  "Town Center": 621,
}

#BUILDING IDS WE DONT CARE ABOUT TO REDUCE NOISE
IGNORE_IDS = {
  "Farm": 50,
  "House":70,
  "Lumber Mill":562,
  "Mining Camp":584,
  "Palisade Gate 1":792,
  "Palisade Gate 2":793,
  "Palisade Gate 3":794,
  "Palisade Gate 4":795,
  "Palisade Gate 5":796,
  "Palisade Gate 6":797,
  "Palisade Gate 7":798,
  "Palisade Gate 8":799,
  "Palisade Gate 9":800,
  "Palisade Gate 10":801,
  "Palisade Gate 11":802,
  "Palisade Gate 12":803,
  "Palisade Gate 13":804,
}

TECH_IDS = {
  "Town Watch": 8,
  "Heavy Plow": 13,
  "Horse Collar":14,
  "Loom":22,
  "Gold Mining":55,
  "Forging":67,
  "Iron Casting":68,
  "Scale Mail Armor":74,
  "Blast Furnace":75,
  "Chain Mail Armor":76,
  "Plate Mail Armor":77,
  "Plate Barding Armor":80,
  "Scale Barding Armor":81,
  "Chain Barding Armor":82,
  "Ballistics":93,
  "Elite Skirmisher":98,
  "Crossbowman":100,
  "Feudal Age":101,
  "Castle Age":102,
  "Imperial Age":103,
  "Gold Shaft Mining": 182,
  "Pikeman": 197,
  "Fletching":199,
  "Bodkin Arrow":200,
  "Double-Bit Axe": 202,
  "Bow Saw": 203,
  "Longsword":207,
  "Padded Archer Armor":211,
  "Leather Archer Armor":212,
  "Wheelbarrow":213,
  "Squires":215,
  "Man-at-Arms":222,
  "Stone Mining":278,
  "Town Patrol": 280,
  "Eagle Warrior":384,
  "Hussar":428,
  "Halberdier":429,
  "Bloodlines":435,
  "Parthian Tactics":436,
  "Thumb Ring":437,
  "Arson":602,
  "Supplies":716,
}

ID_UNITS = {v: k for k, v in UNIT_IDS.items()}
ID_BUILDINGS = {v: k for k, v in BUILDING_IDS.items()}
ID_IGNORE = {v: k for k, v in IGNORE_IDS.items()}
ID_TECHS = {v: k for k, v in TECH_IDS.items()}

def item_in_list(event, list):
  for i in list:
    if i == event:
      return i
  return False

def output_time(millis):
  seconds = math.floor((millis/1000)%60)
  minutes = math.floor((millis/(1000*60)))
  return str(minutes).zfill(2) + ":" + str(seconds).zfill(2)

class Event:
  def __init__(self, event_type, name, timestamp, duration=0):
    self.event_type = event_type
    self.name = name
    self.timestamp = timestamp
    self.duration = duration
  def __str__(self):
    if self.duration:
      return f'{self.event_type.name}: {self.name} {output_time(self.timestamp)} -> {output_time(self.timestamp+self.duration)}'
    else:
      return f'{self.event_type.name}: {self.name} {output_time(self.timestamp)}'
  def __eq__(self, rhs_):
    if(self.event_type == EventType.BUILDING):
      return False
    if(self.event_type == rhs_.event_type and self.name == rhs_.name):
      return True
    return False

def parse_replay(replay_file):
  #lazily init players
  players = []
  for i in range(9):
    #List of event objects for each player
    players.append([])
  
  #load data json
  with open(os.path.join('aoe2techtree', 'data', 'data.json')) as json_file:
    aoe_data = json.load(json_file)
  
  #build civ dict
  civs = {}
  for name,value in aoe_data["civ_names"].items():
    civs[int(value)-10270] = name

  time = 0
  
  with open(replay_file, 'rb') as data:
    eof = os.fstat(data.fileno()).st_size
    h = header.parse_stream(data)
    fast.meta(data)
    while data.tell() < eof:
      o = fast.operation(data)
      if o[0] == fast.Operation.ACTION:
        if o[1][0] == fast.Action.DE_QUEUE:
          player_id = o[1][1]["player_id"]
          unit_id = o[1][1]["unit_id"]
          event = None
          if unit_id in ID_UNITS:
            event = Event(EventType.UNIT, ID_UNITS[unit_id], time)
          elif str(unit_id) in aoe_data["data"]["units"]:
            #unit not found in local records
            name = f'{aoe_data["data"]["units"][str(unit_id)]["internal_name"]} ({unit_id})'
            event = Event(EventType.UNIT, name, time)
          else:
            name = f'{unit_id}'
            event = Event(EventType.UNIT, name, time)
          found_item = item_in_list(event, players[player_id])
          if not found_item:
            players[player_id].append(event)
        
        elif o[1][0] == fast.Action.RESEARCH:
          player_id = o[1][1]["player_id"]
          technology_id = o[1][1]["technology_id"]
          string = ""
          duration = 0
          if str(technology_id) in aoe_data["data"]["techs"]:
            duration = int(aoe_data["data"]["techs"][str(technology_id)]["ResearchTime"]) * 1000
          event = None
          if technology_id in ID_TECHS:
            event = Event(EventType.TECH, ID_TECHS[technology_id], time, duration)
          elif str(technology_id) in aoe_data["data"]["techs"]:
            name = f'{aoe_data["data"]["techs"][str(technology_id)]["internal_name"]} ({technology_id})'
            event = Event(EventType.TECH, name, time, duration)
          else:
            name = f'{technology_id}'
            event = Event(EventType.TECH, name, time, duration)
  
          found_item = item_in_list(event, players[player_id])
          if found_item:
            players[player_id].remove(found_item)
          
          players[player_id].append(event)
        
        elif o[1][0] == fast.Action.BUILD:
          player_id = o[1][1]["player_id"]
          building_id = o[1][1]["building_id"]
          string = ""
          if building_id in ID_BUILDINGS:
            event = Event(EventType.BUILDING, ID_BUILDINGS[building_id], time)
            players[player_id].append(event)
          elif building_id not in ID_IGNORE and building_id in aoe_data["data"]["buildings"]:
            name = f'{aoe_data["data"]["buildings"][str(building_id)]["internal_name"]} ({building_id})'
            event = Event(EventType.BUILDING, name, time)
            players[player_id].append(event)
          elif building_id not in ID_IGNORE:
            name = f'{building_id}'
            event = Event(EventType.BUILDING, name, time)
            players[player_id].append(event)
      elif o[0] == fast.Operation.SYNC:
        time += o[1][0]
  return players, h, civs


def guess_strategy(players, header, civs):
  player_strategies = []
  for player in players:
    mill_event_indexes = []
    militia_event_indexes = []
    archer_event_indexes = []
    scout_event_indexes = []
    skirmisher_event_indexes = []
    blacksmith_event_indexes = []
    feudal_event_indexes = []
    castle_event_indexes = []
    
    archery_range_event_indexes = []
    stable_event_indexes = []
    barracks_event_indexes = []
    
    index = 0
    for event in player:
      if event.event_type == EventType.BUILDING:
        if event.name == "Archery Range":
          archery_range_event_indexes.append(index)
        elif event.name == "Stable":
          stable_event_indexes.append(index)
        elif event.name == "Barracks":
          barracks_event_indexes.append(index)
        elif event.name == "Mill":
          mill_event_indexes.append(index)
        elif event.name == "Blacksmith":
          barracks_event_indexes.append(index)
      elif event.event_type == EventType.UNIT:
        if event.name == "Militia":
          militia_event_indexes.append(index)
        elif event.name == "Archer":
          archer_event_indexes.append(index)
        elif event.name == "Scout":
          scout_event_indexes.append(index)
        elif event.name == "Skirmisher":
          scout_event_indexes.append(index)
      elif event.event_type == EventType.TECH:
        if event.name == "Feudal Age":
          feudal_event_indexes.append(index)
        elif event.name == "Castle Age":
          castle_event_indexes.append(index)
          
      index += 1
    #now analyze to find opening
    strategy = []
    if not feudal_event_indexes:
      strategy = (OpeningType.Unknown)
      continue
    if barracks_event_indexes and mill_event_indexes and militia_event_indexes and militia_event_indexes[0] < feudal_event_indexes[0]:
      if barracks_event_indexes[0] < mill_event_indexes[0]:
        strategy = (OpeningType.PremillDrush)
      elif mill_event_indexes[0] > barracks_event_indexes[0]:
        strategy = (OpeningType.PostmillDrush)
    elif scout_event_indexes and archer_event_indexes and not militia_event_indexes:
      if scout_event_indexes[0] < archer_event_indexes[0]:
        strategy = (OpeningType.Scouts)
      else:
        strategy = (OpeningType.StraightArchers)
    elif scout_event_indexes and not archer_event_indexes and militia_event_indexes:
      if scout_event_indexes[0] < militia_event_indexes[0]:
        strategy = (OpeningType.Scouts)
      #test for maa, drush is already accounted for
      elif militia_event_indexes[0] > feudal_event_indexes[0]:
        strategy = (OpeningType.Maa)
    elif not scout_event_indexes and archer_event_indexes and militia_event_indexes:
      if archer_event_indexes[0] < militia_event_indexes[0]:
        strategy = (OpeningType.StraightArchers)
      #test for maa, drush is already accounted for
      elif militia_event_indexes[0] > feudal_event_indexes[0]:
        strategy = (OpeningType.Maa)
    elif militia_event_indexes and militia_event_indexes[0] > feudal_event_indexes[0]:
      strategy = (OpeningType.Maa)
    #if only archers were made
    elif archer_event_indexes:
      strategy = (OpeningType.StraightArchers)
    #if only scouts were made
    elif scout_event_indexes:
      strategy = (OpeningType.Scouts)
    elif castle_event_indexes:
      strategy = (OpeningType.FastCastle)
    else:
      strategy = OpeningType.Unknown

    player_strategies.append(strategy)
  return player_strategies

      


def print_events(players, header, civs, player_strategies):
  #output to std with information
  print(const.DE_MAP_NAMES[header.de.selected_map_id])
  
  player_num = 0
  for player in players:
    if not player:
      continue
    print("Player: " + header.de.players[player_num].name.value.decode())
    print("Civ: " + civs[header.de.players[player_num].civ_id])
    print("Opener: " + str(player_strategies[player_num]))
    player_num += 1
    for unique_actions in player:
      print(unique_actions)
    print("\n")

if __name__ == '__main__':
  players, header, civs = parse_replay(sys.argv[1])
  player_strategies = guess_strategy(players, header, civs)
  print_events(players, header, civs, player_strategies)
