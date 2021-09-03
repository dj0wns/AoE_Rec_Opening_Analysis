import os
import sys
import math
import json
import io
from mgz import header, fast, enums, const
from mgz.enums import OperationEnum
from construct import Byte
from collections import OrderedDict
from enum import Enum

PARSER_VERSION = 10  #Move to flags system for better resolution
aoe_data = None


class EventType(Enum):
    UNIT = 1
    BUILDING = 2
    TECH = 3
    RESIGN = 4


class OpeningType(Enum):
    # Dark Age
    Unknown = 0
    DidNothing = 0xffffffff
    PremillDrush = 0x1
    PostmillDrush = 0x2

    #Feudal Age
    Maa = 0x100
    FeudalArcherOpening = 0x200
    FeudalScoutOpening = 0x400
    FeudalSkirmOpening = 0x800
    FeudalArcherFollowup = 0x1000
    FeudalScoutFollowup = 0x2000
    FeudalSkirmFollowup = 0x4000
    FeudalTowers = 0x8000
    FeudalEagles = 0x10000

    #Castle Age
    FastCastle = 0x100000
    CastleCrossbows = 0x200000
    CastleKnights = 0x400000
    CastleSiege = 0x800000
    CastleEliteSkirm = 0x1000000
    CastlePikemen = 0x2000000
    CastleEagles = 0x4000000
    CastleCamels = 0x8000000
    CastleUU = 0x10000000

    # Meta Types
    PremillDrushFC = 0x100001
    PostmillDrushFC = 0x100002
    PremillDrushArchers = 0x1001
    PostmillDrushArchers = 0x1002
    PremillDrushSkirms = 0x4001
    PostmillDrushSkirms = 0x4002
    PremillDrushScouts = 0x2001
    PostmillDrushScouts = 0x2002
    PremillDrushMaa = 0x101
    PostmillDrushMaa = 0x102
    MaaArchers = 0x1100
    MaaScouts = 0x2100
    MaaSkirms = 0x4100
    MaaTowers = 0x8100
    MaaEagles = 0x10100
    ScoutsArchers = 0x1400
    ScoutsSkirms = 0x4400


# UNIT IDS #
UNIT_IDS = {
    "Archer": 4,
    "Skirmisher": 7,
    "Knight": 38,
    "Cavalry Archer": 39,
    "Militia": 74,
    "MAA": 75,
    "Villager": 83,
    "Spearman": 93,
    "Monk": 125,
    "Scorpion": 279,
    "Mangonel": 280,
    "Camel": 329,
    "Scout": 448,
    "Eagle": 751,
    "Battering Ram": 1258,
}

# Unique Unit Ids
UNIQUE_UNIT_IDS = {
    "Jaguar Warrior": 725,
    "Camel Archer": 1007,
    "Longbowman": 8,
    "Konnik": 1225,
    "Coustillier": 1655,
    "Arambai": 1126,
    "Cataphract": 40,
    "Woad Raider": 232,
    "Chu Ko Nu": 73,
    "Kipchak": 1231,
    "Shotel Warrior": 1016,
    "Throwing Axeman": 281,
    "Huskarl": 41,
    "Tarkan": 755,
    "Kamayuk": 879,
    "Elephant Archer": 873,
    "Genoese Crossbowman": 866,
    "Samurai": 291,
    "Ballista Elephant": 1120,
    "War Wagon": 827,
    "Leitis": 1234,
    "Huszar": 869,
    "Karambit Warrior": 1123,
    "Gbeto": 1013,
    "Plumed Archer": 763,
    "Mangudai": 11,
    "War Elephant": 239,
    "Organ Gun": 1001,
    "Mameluke": 282,
    "Serjeant": 1660,
    "Boyar": 876,
    "Conquistador": 771,
    "Keshik": 1228,
    "Teutonic Knight": 25,
    "Janissary": 46,
    "Rattan Archer": 1129,
    "Berserk": 692,
    "Obuch": 1701,
    "Hussite Wagon": 1704,
}

#ONLY BUILDINGS WE CARE ABOUT
BUILDING_IDS = {
    "Archery Range": 10,
    "Archery Range": 87,
    "Barracks": 12,
    "Blacksmith 2": 18,
    "Blacksmith 3": 19,
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
    "Blacksmith": 103,
    "Monastery": 104,
    "Town Center 2": 109,
    "Town Center": 621,
}

#BUILDING IDS WE DONT CARE ABOUT TO REDUCE NOISE
IGNORE_IDS = {
    "Farm": 50,
    "House": 70,
    "Lumber Mill": 562,
    "Mining Camp": 584,
    "Palisade Gate 1": 792,
    "Palisade Gate 2": 793,
    "Palisade Gate 3": 794,
    "Palisade Gate 4": 795,
    "Palisade Gate 5": 796,
    "Palisade Gate 6": 797,
    "Palisade Gate 7": 798,
    "Palisade Gate 8": 799,
    "Palisade Gate 9": 800,
    "Palisade Gate 10": 801,
    "Palisade Gate 11": 802,
    "Palisade Gate 12": 803,
    "Palisade Gate 13": 804,
}

TECH_IDS = {
    "Town Watch": 8,
    "Heavy Plow": 13,
    "Horse Collar": 14,
    "Loom": 22,
    "Gold Mining": 55,
    "Forging": 67,
    "Iron Casting": 68,
    "Scale Mail Armor": 74,
    "Blast Furnace": 75,
    "Chain Mail Armor": 76,
    "Plate Mail Armor": 77,
    "Plate Barding Armor": 80,
    "Scale Barding Armor": 81,
    "Chain Barding Armor": 82,
    "Ballistics": 93,
    "Elite Skirmisher": 98,
    "Crossbowman": 100,
    "Feudal Age": 101,
    "Castle Age": 102,
    "Imperial Age": 103,
    "Gold Shaft Mining": 182,
    "Pikeman": 197,
    "Fletching": 199,
    "Bodkin Arrow": 200,
    "Double-Bit Axe": 202,
    "Bow Saw": 203,
    "Longsword": 207,
    "Padded Archer Armor": 211,
    "Leather Archer Armor": 212,
    "Wheelbarrow": 213,
    "Squires": 215,
    "Man-at-Arms": 222,
    "Stone Mining": 278,
    "Town Patrol": 280,
    "Eagle Warrior": 384,
    "Hussar": 428,
    "Halberdier": 429,
    "Bloodlines": 435,
    "Parthian Tactics": 436,
    "Thumb Ring": 437,
    "Arson": 602,
    "Supplies": 716,
}

ID_UNITS = {v: k for k, v in UNIT_IDS.items()}
ID_UNIQUE_UNITS = {v: k for k, v in UNIQUE_UNIT_IDS.items()}
ID_BUILDINGS = {v: k for k, v in BUILDING_IDS.items()}
ID_IGNORE = {v: k for k, v in IGNORE_IDS.items()}
ID_TECHS = {v: k for k, v in TECH_IDS.items()}


def item_in_list(event, list):
    for i in list:
        if i == event:
            return i
    return False


def output_time(millis):
    seconds = math.floor((millis / 1000) % 60)
    minutes = math.floor((millis / (1000 * 60)))
    return str(minutes).zfill(2) + ":" + str(seconds).zfill(2)


class Event:

    def __init__(self, event_type, id, name, timestamp, duration=0):
        self.event_type = event_type
        self.id = id
        if name is None:
            self.update_name()
        else:
            self.name = name
        self.timestamp = timestamp
        self.duration = duration

    def __str__(self):
        if self.duration:
            return f'{self.event_type.name}: {self.name} {output_time(self.timestamp)} -> {output_time(self.timestamp+self.duration)}'
        else:
            return f'{self.event_type.name}: {self.name} {output_time(self.timestamp)}'

    def __eq__(self, rhs_):
        if (self.event_type == EventType.BUILDING):
            return False
        if (self.event_type == rhs_.event_type and self.name == rhs_.name):
            return True
        return False

    def update_name(self):
        global aoe_data
        if aoe_data is None:
            with open(os.path.join('aoe2techtree', 'data',
                                   'data.json')) as json_file:
                aoe_data = json.load(json_file)
        if self.event_type == EventType.UNIT:
            if self.id in ID_UNITS:
                self.name = ID_UNITS[self.id]
            elif str(self.id) in aoe_data["data"]["units"]:
                #unit not found in local records
                self.name = f'{aoe_data["data"]["units"][str(self.id)]["internal_name"]} ({self.id})'
            else:
                self.name = str(self.id)
        elif self.event_type == EventType.TECH:
            if self.id in ID_TECHS:
                self.name = ID_TECHS[self.id]
            elif str(self.id) in aoe_data["data"]["techs"]:
                self.name = f'{aoe_data["data"]["techs"][str(self.id)]["internal_name"]} ({self.id})'
            else:
                self.name = str(self.id)
        elif self.event_type == EventType.BUILDING:
            if self.id in ID_BUILDINGS:
                self.name = ID_BUILDINGS[self.id]
            elif str(self.id) in aoe_data["data"]["buildings"]:
                self.name = f'{aoe_data["data"]["buildings"][str(self.id)]["internal_name"]} ({self.id})'
            else:
                self.name = str(self.id)
        elif self.event_type == EventType.RESIGN:
            self.name = 'Resignation'


def parse_replay(data):
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
    for name, value in aoe_data["civ_names"].items():
        civs[int(value) - 10270] = name

    time = 0

    if type(data) is io.BytesIO:
        eof = len(data.getvalue())
    else:
        eof = os.fstat(data.fileno()).st_size

    h = header.parse_stream(data)
    loser_index = None
    fast.meta(data)
    while data.tell() < eof:
        o = fast.operation(data)
        if o[0] == fast.Operation.ACTION:
            if o[1][0] == fast.Action.DE_QUEUE:
                player_id = o[1][1]["player_id"]
                unit_id = o[1][1]["unit_id"]
                event = None
                if unit_id in ID_UNITS:
                    event = Event(EventType.UNIT, unit_id, ID_UNITS[unit_id],
                                  time)
                elif str(unit_id) in aoe_data["data"]["units"]:
                    #unit not found in local records
                    name = f'{aoe_data["data"]["units"][str(unit_id)]["internal_name"]} ({unit_id})'
                    event = Event(EventType.UNIT, unit_id, name, time)
                else:
                    name = f'{unit_id}'
                    event = Event(EventType.UNIT, unit_id, name, time)
                players[player_id].append(event)

            elif o[1][0] == fast.Action.RESEARCH:
                player_id = o[1][1]["player_id"]
                technology_id = o[1][1]["technology_id"]
                string = ""
                duration = 0
                if str(technology_id) in aoe_data["data"]["techs"]:
                    duration = int(aoe_data["data"]["techs"][str(technology_id)]
                                   ["ResearchTime"]) * 1000
                event = None
                if technology_id in ID_TECHS:
                    event = Event(EventType.TECH, technology_id,
                                  ID_TECHS[technology_id], time, duration)
                elif str(technology_id) in aoe_data["data"]["techs"]:
                    name = f'{aoe_data["data"]["techs"][str(technology_id)]["internal_name"]} ({technology_id})'
                    event = Event(EventType.TECH, technology_id, name, time,
                                  duration)
                else:
                    name = f'{technology_id}'
                    event = Event(EventType.TECH, technology_id, name, time,
                                  duration)

                found_item = item_in_list(event, players[player_id])
                if found_item:
                    players[player_id].remove(found_item)

                players[player_id].append(event)

            elif o[1][0] == fast.Action.BUILD:
                player_id = o[1][1]["player_id"]
                building_id = o[1][1]["building_id"]
                string = ""
                if building_id in ID_BUILDINGS:
                    event = Event(EventType.BUILDING, building_id,
                                  ID_BUILDINGS[building_id], time)
                elif building_id in aoe_data["data"]["buildings"]:
                    name = f'{aoe_data["data"]["buildings"][str(building_id)]["internal_name"]} ({building_id})'
                    event = Event(EventType.BUILDING, building_id, name, time)
                else:
                    name = f'{building_id}'
                    event = Event(EventType.BUILDING, building_id, name, time)
                players[player_id].append(event)

            elif o[1][0] == fast.Action.RESIGN:
                name = 'Resignation'
                player_id = o[1][1]["player_id"]
                event = Event(EventType.RESIGN, 0, name, time)
                players[player_id].append(event)
                loser_index = player_id
        elif o[0] == fast.Operation.SYNC:
            time += o[1][0]

    return players, h, civs, loser_index


def guess_strategy(players):
    player_strategies = []
    for player in players:
        openings = 0
        index = 0
        current_age = 0  # dark = 0, feudal, castle, imp
        mill_built = False
        opening_found = False
        has_archers = False
        has_scouts = False
        has_skirms = False
        barracks_before_mill = False

        for event in player:
            if event.event_type == EventType.TECH:
                if event.name == "Feudal Age":
                    current_age = 1
                elif event.name == "Castle Age":
                    current_age = 2
                    if event.timestamp < 920000:  #15:20 in millis, if clicking now you will land at 18:00
                        openings |= OpeningType.FastCastle.value
                elif event.name == "Imperial Age":
                    #We arent parsing anything in imp
                    current_age = 3
                    break
                elif event.name == "Crossbowman":
                    if current_age == 2:
                        openings |= OpeningType.CastleCrossbows.value
                elif event.name == "Elite Skirmisher":
                    if current_age == 2:
                        openings |= OpeningType.CastleEliteSkirm.value
                elif event.name == "Pikeman":
                    if current_age == 2:
                        openings |= OpeningType.CastlePikemen.value
                elif event.name == "Eagle Warrior":
                    if current_age == 2:
                        openings |= OpeningType.CastleEagles.value
                elif event.name == "Man-at-Arms":
                    if current_age == 1 and (
                            openings == 0x1 or openings == 0x2
                    ):  #specific case where maa is a followup to drush
                        openings |= OpeningType.Maa.value

            elif event.event_type == EventType.BUILDING:
                if event.name == "Mill":
                    mill_built = True

                if event.name == "Barracks":
                    if mill_built:
                        barracks_before_mill = False
                    else:
                        barracks_before_mill = True

                if event.name == "Tower":
                    if current_age == 1:
                        openings |= FeudalTowers.value

            elif event.event_type == EventType.UNIT:
                if event.name == "Militia":
                    if opening_found:
                        continue
                    if current_age == 0 and barracks_before_mill:
                        openings |= OpeningType.PremillDrush.value
                    elif current_age == 0:
                        openings |= OpeningType.PostmillDrush.value
                    elif current_age == 1:
                        openings |= OpeningType.Maa.value
                    opening_found = True  # First unit made is the opening, otherwise we dont care about maa i think

                elif event.name == "Archer":
                    # Only count archers once
                    if has_archers:
                        continue
                    has_archers = True
                    if current_age == 1:
                        if not opening_found:
                            openings |= OpeningType.FeudalArcherOpening.value
                        else:
                            openings |= OpeningType.FeudalArcherFollowup.value
                        # Use xbow tech to determine if they did xbow in castle
                    opening_found = True  # First unit made is the opening, otherwise we dont care about maa i think

                elif event.name == "Scout":
                    # Only count scouts once
                    if has_scouts:
                        continue
                    has_scouts = True
                    if current_age == 1:
                        if not opening_found:
                            openings |= OpeningType.FeudalScoutOpening.value
                        else:
                            openings |= OpeningType.FeudalScoutFollowup.value
                    opening_found = True  # First unit made is the opening, otherwise we dont care about maa i think

                elif event.name == "Skirmisher":
                    # Only count skirms once
                    if has_skirms:
                        continue
                    has_skirms = True
                    if current_age == 1:
                        if not opening_found:
                            openings |= OpeningType.FeudalSkirmOpening.value
                        else:
                            openings |= OpeningType.FeudalSkirmFollowup.value
                        # Use eskirm tech to determine if they did xbow in castle
                    opening_found = True  # First unit made is the opening, otherwise we dont care about maa i think

                elif event.name == "Eagle":
                    if current_age == 1:
                        openings |= OpeningType.FeudalEagles.value
                    opening_found = True

                elif event.name == "Knight":
                    if current_age == 2:
                        openings |= OpeningType.CastleKnights.value

                elif event.name == "Camel":
                    if current_age == 2:
                        openings |= OpeningType.CastleCamels.value

                elif event.name == "Mangonel":
                    if current_age == 2:
                        openings |= OpeningType.CastleSiege.value

                elif event.name == "Scorpion":
                    if current_age == 2:
                        openings |= OpeningType.CastleSiege.value

                elif event.name == "Battering Ram":
                    if current_age == 2:
                        openings |= OpeningType.CastleSiege.value
                elif event.id in ID_UNIQUE_UNITS:
                    if current_age == 2:
                        openings |= OpeningType.CastleUU.value
        if not openings:
            openings = OpeningType.DidNothing.value
        player_strategies.append(openings)
    return player_strategies


def print_events(players, header, civs, player_strategies):
    #output to std with information
    if header is not None:
        print(const.DE_MAP_NAMES[header.de.selected_map_id])

    player_num = 0
    for player in players:
        if not player:
            continue
        if header is not None:
            print("Player: " +
                  header.de.players[player_num].name.value.decode())
            print("Civ: " + civs[header.de.players[player_num].civ_id])
        print("Opener: " + str(player_strategies[player_num]))
        player_num += 1
        for unique_actions in player:
            print(unique_actions)
        print("\n")


if __name__ == '__main__':
    players, header, civs, loser_id = parse_replay(sys.argv[1])
    player_strategies = guess_strategy(players)
    print(loser_id, "Lost")
    print_events(players, header, civs, player_strategies)
