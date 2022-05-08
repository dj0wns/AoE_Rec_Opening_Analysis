import os
import json
import argparse

import parse_replays_and_store_in_db
from aoe_replay_stats import output_time, OpeningType

aoe_data = None

Allowed_Strategies = [
    #General Openings
    [
        "PremillDrush (Any)", [OpeningType.PremillDrush.value],
        [OpeningType.Unknown.value]
    ],
    [
        "PostmillDrush (Any)", [OpeningType.PostmillDrush.value],
        [OpeningType.Unknown.value]
    ],
    ["MAA (Any)", [OpeningType.Maa.value], [OpeningType.AnyDrush.value]],
    [
        "Scouts (Any)", [OpeningType.FeudalScoutOpening.value],
        [OpeningType.Unknown.value]
    ],
    [
        "Range Opener (Any)",
        [
            OpeningType.FeudalArcherOpening.value,
            OpeningType.FeudalSkirmOpening.value
        ], [OpeningType.Unknown.value]
    ],
    #Specific Openings and followups
    [
        "Premill Drush FC", [OpeningType.PremillDrushFC.value],
        [OpeningType.Unknown.value]
    ],
    [
        "Postmill Drush FC", [OpeningType.PostmillDrushFC.value],
        [OpeningType.Unknown.value]
    ],
    [
        "Premill Drush Range Followup",
        [
            OpeningType.PremillDrushArchers.value,
            OpeningType.PremillDrushSkirms.value
        ], [OpeningType.FeudalScoutFollowup.value]
    ],  #disallow scouts
    [
        "Postmill Drush Range Followup",
        [
            OpeningType.PostmillDrushArchers.value,
            OpeningType.PostmillDrushSkirms.value
        ], [OpeningType.FeudalScoutFollowup.value]
    ],  #disallow scouts
    #[
    #    "Premill Drush Scout Followup", [OpeningType.PremillDrushScouts.value],
    #    [
    #        OpeningType.FeudalArcherFollowup.value,
    #        OpeningType.FeudalSkirmFollowup.value
    #    ]
    #],  #disallow range followup
    #[
    #    "Postmill Drush Scout Followup",
    #    [OpeningType.PostmillDrushScouts.value],
    #    [
    #        OpeningType.FeudalArcherFollowup.value,
    #        OpeningType.FeudalSkirmFollowup.value
    #    ]
    #],  #disallow range followup
    [
        "Scouts (No Feudal Followup)", [OpeningType.FeudalScoutOpening.value],
        [
            OpeningType.FeudalArcherFollowup.value,
            OpeningType.FeudalSkirmFollowup.value
        ]
    ],
    [
        "Scouts Range Followup",
        [OpeningType.ScoutsArchers.value, OpeningType.ScoutsSkirms.value],
        [OpeningType.Unknown.value]
    ],
    [
        "MAA (No Feudal Followup)", [OpeningType.Maa.value],
        [
            OpeningType.FeudalArcherFollowup.value,
            OpeningType.FeudalSkirmFollowup.value,
            OpeningType.FeudalScoutFollowup.value,
            OpeningType.FeudalEagles.value
        ]
    ],
    [
        "MAA Range Followup",
        [OpeningType.MaaSkirms.value, OpeningType.MaaArchers.value],
        [OpeningType.FeudalScoutFollowup.value, OpeningType.FeudalEagles.value]
    ],
    #[
    #    "MAA Scout Followup", [OpeningType.MaaScouts.value],
    #    [
    #        OpeningType.FeudalArcherFollowup.value,
    #        OpeningType.FeudalSkirmFollowup.value,
    #        OpeningType.FeudalEagles.value
    #    ]
    #],
    #[
    #    "MAA Eagle Followup", [OpeningType.MaaEagles.value],
    #    [
    #        OpeningType.FeudalArcherFollowup.value,
    #        OpeningType.FeudalSkirmFollowup.value,
    #        OpeningType.FeudalScoutFollowup.value
    #    ]
    #],
]


def arguments_to_query_string(match_table_tag, match_playera_table_tag,
                              match_playerb_table_tag, minimum_elo, maximum_elo,
                              map_ids, include_civ_ids, clamp_civ_ids,
                              no_mirror, exclude_civ_ids, include_ladder_ids,
                              include_patch_ids, clamp_player1, player_ids):
    string = "("
    string += f'{match_table_tag}.average_elo > {minimum_elo}\n'
    string += f'  AND {match_table_tag}.average_elo < {maximum_elo}\n'

    # ignore all did nothing results
    string += f'  AND {match_playera_table_tag}.opening_id != {OpeningType.DidNothing.value}\n'
    string += f'  AND {match_playerb_table_tag}.opening_id != {OpeningType.DidNothing.value}\n'

    if no_mirror:
        string += f'  AND {match_playera_table_tag}.civilization != {match_playerb_table_tag}.civilization\n'

    if map_ids is not None:
        string += '  AND ('
        for i in range(len(map_ids)):
            for j in range(len(map_ids[i])):
                if i + j > 0:
                    string += '    OR '
                string += f'{match_table_tag}.map_id = {map_ids[i][j]}\n'
        string += '       )\n'

    if include_civ_ids:
        string += '  AND ('
        for i in range(len(include_civ_ids)):
            if i > 0:
                string += '    OR '
            string += f'{match_playera_table_tag}.civilization = {include_civ_ids[i]}\n'
            if not clamp_player1:
                string += f'    OR {match_playerb_table_tag}.civilization = {include_civ_ids[i]}\n'
        string += '       )\n'

    if clamp_civ_ids:
        string += '  AND ('
        for i in range(len(clamp_civ_ids)):
            if i > 0:
                string += '    OR '
            string += f'{match_playera_table_tag}.civilization = {clamp_civ_ids[i]}\n'
        string += '       )\n  AND ('
        for i in range(len(clamp_civ_ids)):
            if i > 0:
                string += '    OR '
            string += f'{match_playerb_table_tag}.civilization = {clamp_civ_ids[i]}\n'
        string += '       )\n'
    string += ")"

    if exclude_civ_ids:
        string += '  AND ('
        for i in range(len(exclude_civ_ids)):
            if i > 0:
                string += '    AND '
            string += f'{match_playera_table_tag}.civilization != {exclude_civ_ids[i]}\n'
            string += f'    AND {match_playerb_table_tag}.civilization != {exclude_civ_ids[i]}\n'
        string += '       )\n'

    if include_ladder_ids is not None:
        string += '  AND ('
        for i in range(len(include_ladder_ids)):
            for j in range(len(include_ladder_ids[i])):
                if i + j > 0:
                    string += '    OR '
                string += f'{match_table_tag}.ladder_id = {include_ladder_ids[i][j]}\n'
        string += '       )\n'

    if include_patch_ids is not None:
        string += '  AND ('
        for i in range(len(include_patch_ids)):
            for j in range(len(include_patch_ids[i])):
                if i + j > 0:
                    string += '    OR '
                string += f'{match_table_tag}.patch_id = {include_patch_ids[i][j]}\n'
        string += '       )\n'

    if player_ids is not None:
        string += '  AND ('
        for i in range(len(player_ids)):
            for j in range(len(player_ids[i])):
                if i + j > 0:
                    string += '    OR '
                string += f'{match_playera_table_tag}.player_id = {player_ids[i][j]}\n'
                if not clamp_player1:
                    string += f'    OR {match_playerb_table_tag}.player_id = {player_ids[i][j]}\n'
        string += '       )\n'

    return string


def opening_matchups(opening1, opening2, minimum_elo, maximum_elo, map_ids,
                     include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids,
                     include_ladder_ids, include_patch_ids, player_ids):
    query = """SELECT
               sum(CASE WHEN a.victory = 1 OR a.victory = 0 THEN 1 ELSE 0 END) as Total,
               sum(CASE WHEN a.victory = 1 THEN 1 ELSE 0 END) AS FirstOpeningWins,
               sum(CASE WHEN b.victory = 1 THEN 1 ELSE 0 END) AS SecondOpeningWins,
               sum(CASE WHEN a.victory = -1 THEN 1 ELSE 0 END) AS Unknown
             FROM matches m
             JOIN match_players a ON a.match_id = m.id
             JOIN match_players b ON b.match_id = m.id
             WHERE ("""
    #strat 1 inclusions
    count = 0
    for i in opening1[1]:
        count += 1
        query += f'((a.opening_id & {i}) = {i})'
        if count < len(opening1[1]):
            query += ' OR '
    query += ') AND ('
    #Strat 1 exclusions
    count = 0
    for i in opening1[2]:
        count += 1
        query += f'(NOT (a.opening_id & {i}))'
        if count < len(opening1[2]):
            query += ' AND '
    query += ') AND ('
    #strat 2 inclusions
    count = 0
    for i in opening2[1]:
        count += 1
        query += f'((b.opening_id & {i}) = {i})'
        if count < len(opening2[1]):
            query += ' OR '
    query += ') AND ('
    #Strat 2 exclusions
    count = 0
    for i in opening2[2]:
        count += 1
        query += f'(NOT (b.opening_id & {i}))'
        if count < len(opening2[2]):
            query += ' AND '

    query += ')'
    query += """
               AND a.id != b.id
               AND """
    query += arguments_to_query_string(
        'm', 'a', 'b', minimum_elo, maximum_elo, map_ids, include_civ_ids,
        clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids,
        include_patch_ids, include_civ_ids or player_ids is not None,
        player_ids)
    query += ';'
    return parse_replays_and_store_in_db.connect_and_return(query, ())[0]


def mirror_matchups(opening1, minimum_elo, maximum_elo, map_ids,
                    include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids,
                    include_ladder_ids, include_patch_ids, player_ids):
    query = """SELECT COUNT(a.id)
             FROM matches m
             JOIN match_players a ON a.match_id = m.id
             JOIN match_players b ON b.match_id = m.id
             WHERE ("""
    #strat 1 inclusions
    count = 0
    for i in opening1[1]:
        count += 1
        query += f'((a.opening_id & {i}) = {i})'
        if count < len(opening1[1]):
            query += ' OR '
    query += ') AND ('
    #Strat 1 exclusions
    count = 0
    for i in opening1[2]:
        count += 1
        query += f'(NOT (a.opening_id & {i}))'
        if count < len(opening1[2]):
            query += ' AND '
    query += ')'
    query += """  AND a.id != b.id
                 AND a.victory = 1
                 AND"""
    query += arguments_to_query_string(
        'm', 'a', 'b', minimum_elo, maximum_elo, map_ids, include_civ_ids,
        clamp_civ_ids, no_mirror, exclude_civ_ids, include_ladder_ids,
        include_patch_ids, include_civ_ids or player_ids is not None,
        player_ids)
    query += ';'
    return parse_replays_and_store_in_db.connect_and_return(query, ())[0]


#Clamps to included civs!
def age_up_times_for_opening(opening1, minimum_elo, maximum_elo, map_ids,
                             include_civ_ids, clamp_civ_ids, no_mirror,
                             exclude_civ_ids, include_ladder_ids,
                             include_patch_ids, player_ids, tech_ids):
    query = """SELECT a.id, c.event_id, c.time, c.duration
             FROM matches m
             JOIN match_players a ON a.match_id = m.id
             JOIN match_players b ON b.match_id = m.id
             JOIN match_player_actions c ON a.id = c.match_player_id
             WHERE ("""
    #strat 1 inclusions
    count = 0
    for i in opening1[1]:
        count += 1
        query += f'((a.opening_id & {i}) = {i})'
        if count < len(opening1[1]):
            query += ' OR '
    query += ') AND ('
    #Strat 1 exclusions
    count = 0
    for i in opening1[2]:
        count += 1
        query += f'(NOT (a.opening_id & {i}))'
        if count < len(opening1[2]):
            query += ' AND '
    query += ')'
    query += """AND a.id != b.id
               AND c.event_type = 3
               AND (c.event_id = 101
                 OR c.event_id = 102
                 OR c.event_id = 103"""
    if tech_ids is not None:
        query += '    OR '
        for i in range(len(tech_ids)):
            for j in range(len(tech_ids[i])):
                if i + j > 0:
                    query += '    OR '
                query += f'c.event_id = {tech_ids[i][j]}\n'
    query += ") "
    query += " AND "
    query += arguments_to_query_string('m', 'a', 'b', minimum_elo, maximum_elo,
                                       map_ids, include_civ_ids, clamp_civ_ids,
                                       no_mirror, exclude_civ_ids,
                                       include_ladder_ids, include_patch_ids,
                                       True, player_ids)
    query += 'ORDER BY a.id;'
    return parse_replays_and_store_in_db.connect_and_return(query, ())


def total_concluded_matches(minimum_elo, maximum_elo, map_ids, include_civ_ids,
                            clamp_civ_ids, no_mirror, exclude_civ_ids,
                            include_ladder_ids, include_patch_ids, player_ids):
    query = """SELECT COUNT(a.id)
             FROM match_players a
             JOIN matches m ON m.id = a.match_id
             join match_players b on a.match_id = b.match_id
             WHERE a.victory = 1
               AND a.id != b.id
               AND """
    query += arguments_to_query_string('m', 'a', 'b', minimum_elo, maximum_elo,
                                       map_ids, include_civ_ids, clamp_civ_ids,
                                       no_mirror, exclude_civ_ids,
                                       include_ladder_ids, include_patch_ids,
                                       False, player_ids)
    if no_mirror:
        query += """AND a.civilization != b.civilization"""
    query += ';'
    return parse_replays_and_store_in_db.connect_and_return(query, ())[0][0]


def get_strategies():
    query = """SELECT id, name from openings ORDER BY name;"""
    return parse_replays_and_store_in_db.connect_and_return(query, ())


def get_civilizations():
    query = """SELECT DISTINCT civilization
             FROM match_players;"""
    return parse_replays_and_store_in_db.connect_and_return(query, ())


def get_civilization_count(civ_id, minimum_elo, maximum_elo, map_ids,
                           include_civ_ids, clamp_civ_ids, no_mirror,
                           exclude_civ_ids, include_ladder_ids,
                           include_patch_ids, player_ids):
    query = """SELECT
               sum(CASE WHEN a.victory = 1 OR a.victory = 0 THEN 1 ELSE 0 END) as Total,
               sum(CASE WHEN a.victory = 1 THEN 1 ELSE 0 END) as Wins,
               sum(CASE WHEN a.victory = 0 THEN 1 ELSE 0 END) as Losses
             FROM match_players a
             JOIN matches m ON m.id = a.match_id
             join match_players b on a.match_id = b.match_id
             WHERE a.civilization = ?
             AND (a.victory = 1
             OR a.victory = 0)
             AND a.id != b.id
             AND"""

    query += arguments_to_query_string('m', 'a', 'b', minimum_elo, maximum_elo,
                                       map_ids, include_civ_ids, clamp_civ_ids,
                                       no_mirror, exclude_civ_ids,
                                       include_ladder_ids, include_patch_ids,
                                       player_ids is not None, player_ids)
    query += ';'
    return parse_replays_and_store_in_db.connect_and_return(query, (civ_id,))[0]


def print_uptimes_per_strategy(total_matches, minimum_elo, maximum_elo, map_ids,
                               include_civ_ids, clamp_civ_ids, no_mirror,
                               exclude_civ_ids, include_ladder_ids,
                               include_patch_ids, player_ids, tech_ids):
    print('Uptimes and Selected technologies!')
    #average times through the ages
    translated_names = {101: "Feudal Age", 102: "Castle Age"}
    for i in range(len(Allowed_Strategies)):
        age_up_times = age_up_times_for_opening(
            Allowed_Strategies[i], minimum_elo, maximum_elo, map_ids,
            include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids,
            include_ladder_ids, include_patch_ids, player_ids, tech_ids)
        if not age_up_times:
            continue
        research_dict = {}

        for event in age_up_times:
            if event[1] in research_dict:
                research_dict[event[1]]["time"] += event[2]
                research_dict[event[1]]["count"] += 1
            else:
                research_dict[event[1]] = {"time": event[2], "count": 1}
        count = sorted(research_dict.items())[0][1][
            "count"]  #use feudal count for each strategy because its pretty much guaranteed
        string = f'{Allowed_Strategies[i][0]} ({count}): '
        for k, v in sorted(research_dict.items()):
            if k in translated_names:
                string += translated_names[k] + ": "
            else:
                string += aoe_data["data"]["techs"][str(
                    k)]["internal_name"] + ": "
            string += output_time(
                v["time"] / v["count"] +
                int(aoe_data["data"]["techs"][str(k)]["ResearchTime"]) * 1000)
            string += ", "
        print(string)


def print_strategy_matchups(total_matches, minimum_elo, maximum_elo, map_ids,
                            include_civ_ids, clamp_civ_ids, no_mirror,
                            exclude_civ_ids, include_ladder_ids,
                            include_patch_ids, player_ids, tech_ids):
    print("\nStrategy Matchups!")

    for i in range(len(Allowed_Strategies)):
        for j in range(len(Allowed_Strategies)):
            if i == j:
                if include_civ_ids or player_ids is not None:
                    total, firstwins, secondwins, unknown = opening_matchups(
                        Allowed_Strategies[i], Allowed_Strategies[j],
                        minimum_elo, maximum_elo, map_ids, include_civ_ids,
                        clamp_civ_ids, no_mirror, exclude_civ_ids,
                        include_ladder_ids, include_patch_ids, player_ids)
                    if total:
                        print(
                            f'{Allowed_Strategies[i][0]} vs {Allowed_Strategies[j][0]} - {total} ({total/total_matches:.1%}), {firstwins}:{secondwins} ({firstwins/total:.1%}:{secondwins/total:.1%}) with {unknown} unknowns'
                        )
                else:
                    total = mirror_matchups(Allowed_Strategies[i], minimum_elo,
                                            maximum_elo, map_ids,
                                            include_civ_ids, clamp_civ_ids,
                                            no_mirror, exclude_civ_ids,
                                            include_ladder_ids,
                                            include_patch_ids, player_ids)[0]
                    if total:
                        print(
                            f'{Allowed_Strategies[i][0]} vs {Allowed_Strategies[i][0]} - {total} ({total/total_matches:.1%})'
                        )

            else:
                total, firstwins, secondwins, unknown = opening_matchups(
                    Allowed_Strategies[i], Allowed_Strategies[j], minimum_elo,
                    maximum_elo, map_ids, include_civ_ids, clamp_civ_ids,
                    no_mirror, exclude_civ_ids, include_ladder_ids,
                    include_patch_ids, player_ids)
                if total:
                    print(
                        f'{Allowed_Strategies[i][0]} vs {Allowed_Strategies[j][0]} - {total} ({total/total_matches:.1%}), {firstwins}:{secondwins} ({firstwins/total:.1%}:{secondwins/total:.1%}) with {unknown} unknowns'
                    )


def print_civ_stats(total_matches, minimum_elo, maximum_elo, map_ids,
                    include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids,
                    include_ladder_ids, include_patch_ids, player_ids,
                    tech_ids):
    #Go through civilizations
    print('\nCivilization Stats!')
    civilizations = get_civilizations()

    #build civ dict
    civs = {}
    for name, value in aoe_data["civ_names"].items():
        civs[int(value) - 10270] = name

    total_matches = total_concluded_matches(minimum_elo, maximum_elo, map_ids,
                                            include_civ_ids, clamp_civ_ids,
                                            no_mirror, exclude_civ_ids,
                                            include_ladder_ids,
                                            include_patch_ids, player_ids)
    for i in range(len(civilizations)):
        total, wins, losses = get_civilization_count(
            civilizations[i][0], minimum_elo, maximum_elo, map_ids,
            include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids,
            include_ladder_ids, include_patch_ids, player_ids)
        if total:
            #divide play rate by 2 because there are 2 civs chosen for every match!
            print(
                f'{civs[civilizations[i][0]]} - {total} ({total/total_matches/2.:.1%}), {wins}:{losses} ({wins/total:.1%})'
            )


def execute(minimum_elo, maximum_elo, map_ids, include_civ_ids, clamp_civ_ids,
            no_mirror, exclude_civ_ids, include_ladder_ids, include_patch_ids,
            player_ids, tech_ids):
    total_matches = total_concluded_matches(minimum_elo, maximum_elo, map_ids,
                                            include_civ_ids, clamp_civ_ids,
                                            no_mirror, exclude_civ_ids,
                                            include_ladder_ids,
                                            include_patch_ids, player_ids)
    print(f'{total_matches} matches in query!\n')

    if not total_matches:
        print("No matches found matching the criteria")
        return

    print_uptimes_per_strategy(total_matches, minimum_elo, maximum_elo, map_ids,
                               include_civ_ids, clamp_civ_ids, no_mirror,
                               exclude_civ_ids, include_ladder_ids,
                               include_patch_ids, player_ids, tech_ids)

    print_strategy_matchups(total_matches, minimum_elo, maximum_elo, map_ids,
                            include_civ_ids, clamp_civ_ids, no_mirror,
                            exclude_civ_ids, include_ladder_ids,
                            include_patch_ids, player_ids, tech_ids)

    print_civ_stats(total_matches, minimum_elo, maximum_elo, map_ids,
                    include_civ_ids, clamp_civ_ids, no_mirror, exclude_civ_ids,
                    include_ladder_ids, include_patch_ids, player_ids, tech_ids)


def names_to_ids(include_civ_ids, clamp_civ_ids, exclude_civ_ids):
    new_include_civ_ids = []
    new_clamp_civ_ids = []
    new_exclude_civ_ids = []
    if include_civ_ids is not None:
        for i in range(len(include_civ_ids)):
            for j in range(len(include_civ_ids[i])):
                if include_civ_ids[i][j] in aoe_data["civ_names"]:
                    new_include_civ_ids.append(
                        int(aoe_data["civ_names"][include_civ_ids[i][j]]) -
                        10270)
                else:
                    print(f'{include_civ_ids[i][j]} is not a valid civ name!')
                    exit(0)
    if clamp_civ_ids is not None:
        for i in range(len(clamp_civ_ids)):
            for j in range(len(clamp_civ_ids[i])):
                if clamp_civ_ids[i][j] in aoe_data["civ_names"]:
                    new_clamp_civ_ids.append(
                        int(aoe_data["civ_names"][clamp_civ_ids[i][j]]) - 10270)
                else:
                    print(f'{clamp_civ_ids[i][j]} is not a valid civ name!')
                    exit(0)
    if exclude_civ_ids is not None:
        for i in range(len(exclude_civ_ids)):
            for j in range(len(exclude_civ_ids[i])):
                if exclude_civ_ids[i][j] in aoe_data["civ_names"]:
                    new_exclude_civ_ids.append(
                        int(aoe_data["civ_names"][exclude_civ_ids[i][j]]) -
                        10270)
                else:
                    print(f'{exclude_civ_ids[i][j]} is not a valid civ name!')
                    exit(0)
    return new_include_civ_ids, new_clamp_civ_ids, new_exclude_civ_ids


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Build tailored statistics from the replay database")
    parser.add_argument("-e",
                        "--minimum-elo",
                        help="Minimum match elo for all results",
                        type=int,
                        default=0)
    parser.add_argument("-E",
                        "--maximum-elo",
                        help="Maximum match elo for all results",
                        type=int,
                        default=9999)
    parser.add_argument(
        "-l",
        "--include-ladder-ids",
        help="Only include games played on these ladders (3=1v1, 13=EW)",
        type=int,
        action='append',
        nargs='+')
    parser.add_argument(
        "-p",
        "--include-patch-ids",
        help=
        "Only include games played on these patches (current as of writing this is 25.01)",
        type=str,
        action='append',
        nargs='+')
    parser.add_argument("-m",
                        "--map-ids",
                        help="Restrict all results to these map ids",
                        type=int,
                        action='append',
                        nargs='+')
    parser.add_argument(
        "-c",
        "--include-civ-names",
        help="Include any matches with at least 1 of these civs",
        type=str,
        action='append',
        nargs='+')
    parser.add_argument(
        "-C",
        "--clamp-civ-names",
        help="Only include games where matches only have civs in this pool",
        type=str,
        action='append',
        nargs='+')
    parser.add_argument("-x",
                        "--exclude-civ-names",
                        help="Remove games where these civs are present",
                        type=str,
                        action='append',
                        nargs='+')
    parser.add_argument(
        "-i",
        "--player-ids",
        help="Restrict all results to games with these player ids",
        type=int,
        action='append',
        nargs='+')
    parser.add_argument(
        "-t",
        "--tech-ids",
        help="Include these tech ids in the average research time section",
        type=int,
        action='append',
        nargs='+')
    parser.add_argument("-n",
                        "--no-mirror",
                        help="Remove games where there are mirror matches",
                        action='store_true')
    parser.add_argument("-d",
                        "--db-name",
                        help="Create stats from this db instead",
                        type=str)
    args = parser.parse_args()

    if args.db_name is not None:
        parse_replays_and_store_in_db.DB_FILE = args.db_name
    with open(os.path.join('aoe2techtree', 'data', 'data.json')) as json_file:
        aoe_data = json.load(json_file)
    include_civ_ids, clamp_civ_ids, exclude_civ_ids = names_to_ids(
        args.include_civ_names, args.clamp_civ_names, args.exclude_civ_names)
    execute(args.minimum_elo, args.maximum_elo, args.map_ids, include_civ_ids,
            clamp_civ_ids, args.no_mirror, exclude_civ_ids,
            args.include_ladder_ids, args.include_patch_ids, args.player_ids,
            args.tech_ids)
