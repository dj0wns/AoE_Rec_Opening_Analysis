import sqlite3
import aoe_replay_stats
import grab_replays_for_player
import os
import re
import sys
import argparse

fpath = os.path.realpath(__file__)
path = os.path.dirname(fpath)
DB_FILE = path + "/local.db"


def init_db():
    sql_commands = []
    sql_commands.append("PRAGMA foreign_keys = ON;")
    sql_commands.append(""" CREATE TABLE IF NOT EXISTS openings (
                            id integer NOT NULL PRIMARY KEY,
                            name text NOT NULL
                            ) WITHOUT ROWID; """)

    for opening in aoe_replay_stats.OpeningType:
        sql_commands.append(
            f'INSERT OR IGNORE INTO openings (id, name) VALUES({opening.value}, "{opening.name}")'
        )

    sql_commands.append(""" CREATE TABLE IF NOT EXISTS matches (
                            id integer NOT NULL PRIMARY KEY,
                            average_elo integer NOT NULL,
                            map_id integer NOT NULL,
                            time datetime DEFAULT CURRENT_TIMESTAMP,
                            patch_id float DEFAULT 25.01,
                            ladder_id integer DEFAULT 3
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


def update_schema():
    # First update to db, adding patch number and ladder id to matches
    connect_and_modify(
        """ALTER TABLE matches ADD COLUMN patch_id float DEFAULT 25.01;""", ())
    connect_and_modify(
        """ALTER TABLE matches ADD COLUMN ladder_id integer DEFAULT 3;""", ())


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


def connect_and_modify_with_list(operations):
    try:
        conn = sqlite3.connect(DB_FILE)
        c = conn.cursor()
        for statement, args in operations:
            c.execute(statement, args)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()


def add_player(player_id):
    return ("INSERT OR IGNORE INTO players(id) VALUES(?)", (player_id,))


def add_match(match_id, average_elo, map_id, patch_id, ladder_id, time=False):
    if time:
        return (
            "INSERT OR IGNORE INTO matches(id, average_elo, map_id, patch_id, ladder_id, time) VALUES(?,?,?,?,?,?)",
            (match_id, average_elo, map_id, patch_id, ladder_id, time))
    else:
        return (
            "INSERT OR IGNORE INTO matches(id, average_elo, map_id, patch_id, ladder_id) VALUES(?,?,?,?,?)",
            (match_id, average_elo, map_id, patch_id, ladder_id))


def get_matches():
    matches = connect_and_return("SELECT * FROM matches", ())
    if len(matches) == 0:
        return None
    return matches


def update_match_player(opening_id, match_player_id):
    connect_and_modify(
        """UPDATE match_players
                        SET opening_id = ?, parser_version = ?, time_parsed = CURRENT_TIMESTAMP
                        WHERE id = ?""",
        (opening_id, aoe_replay_stats.PARSER_VERSION, match_player_id))


def add_unparsed_match_player(player_id, match_id, civilization, victory):
    connect_and_modify(
        """INSERT OR IGNORE INTO match_players(player_id, match_id, civilization, victory) VALUES
                            (?,?,?,?)""",
        (player_id, match_id, civilization, victory))


def match_player_actions_generator(match_player_id, action_list):
    statement = """INSERT OR IGNORE INTO match_player_actions
                   (match_player_id, event_type, event_id, time, duration)
                   VALUES (?,?,?,?,?)"""
    for unique_action in action_list:
        yield statement, (match_player_id, unique_action.event_type.value,
                          unique_action.id, unique_action.timestamp,
                          unique_action.duration)


def match_player_actions_generator_from_actions(match_player_actions,
                                                match_player_id):
    statement = """INSERT OR IGNORE INTO match_player_actions
                   (match_player_id, event_type, event_id, time, duration)
                   VALUES (?,?,?,?,?)"""
    for id_old, match_player_id_old, event_type, event_id, time, duration in match_player_actions:
        yield statement, (match_player_id, event_type, event_id, time, duration)


def get_match_player_actions(match_player_id):
    match_player_actions = connect_and_return(
        "SELECT * FROM match_player_actions WHERE match_player_id = ?",
        (match_player_id,))
    if len(match_player_actions) == 0:
        return None
    return match_player_actions


def add_match_player_actions(match_player_id, action_list):
    generator = match_player_actions_generator(match_player_id, action_list)
    connect_and_modify_with_generator(generator)


def does_match_exist(match_id):
    match = connect_and_return("SELECT * FROM matches WHERE id=?", (match_id,))
    if len(match) == 0:
        return False
    return True


def get_match_players(match_id):
    match_players = connect_and_return(
        "SELECT * FROM match_players WHERE match_id = ?", (match_id,))
    return match_players


def get_match_player_id(player_id, match_id):
    match_player_id = connect_and_return(
        "SELECT id FROM match_players WHERE player_id = ? AND match_id = ?",
        (player_id, match_id))
    if len(match_player_id) == 0:
        return None
    return match_player_id[0][0]


def get_match_players_needing_update():
    match_players = connect_and_return(
        "SELECT * FROM match_players WHERE parser_version < ?",
        (aoe_replay_stats.PARSER_VERSION,))
    if len(match_players) == 0:
        return None
    return match_players


def get_actions_for_match_player(match_player_id):
    match_player_actions = connect_and_return(
        "SELECT * FROM match_player_actions WHERE match_player_id = ?",
        (match_player_id,))
    if len(match_player_actions) == 0:
        return None
    return match_player_actions


def parse_replay_file(match_id, player1_id, player2_id, average_elo, ladder_id,
                      file_data):
    #match already in db!
    if does_match_exist(match_id):
        return False
    try:
        players, header, civs, loser_id = aoe_replay_stats.parse_replay(
            file_data)
    except Exception as e:
        print(e)
        return False
    player_ids = [player1_id, player2_id]
    player_num = 0

    #now plug it into the db!
    #first add players
    operations = []
    operations.append(add_player(player1_id))
    operations.append(add_player(player2_id))
    operations.append(
        add_match(match_id, average_elo, header.de.selected_map_id,
                  header.save_version, ladder_id))

    connect_and_modify_with_list(operations)
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
        add_unparsed_match_player(player_ids[player_num], match_id,
                                  header.de.players[player_num].civ_id,
                                  winner_value)
        match_player_id = get_match_player_id(player_ids[player_num], match_id)
        #now add player actions
        add_match_player_actions(match_player_id, players[i])
        player_num += 1

    return True


def import_from_db(input_db):
    # Since we are using a global DB handle im going to do some weird hacky stuff to not rewrite routines
    global DB_FILE
    print(f'Writing from {input_db} to {DB_FILE}')
    output_db = DB_FILE
    DB_FILE = input_db
    #first get all matches
    matches = get_matches()
    #now iterate through matches and add them to other db
    i = 0
    for match in matches:
        print(f'( {i} / {len(matches)} )')
        i += 1
        match_id, average_elo, map_id, time, patch_id, ladder_id = match

        #Check if match_id exists in output db
        DB_FILE = output_db
        if does_match_exist(match_id):
            continue
        DB_FILE = input_db
        operations = []
        #now get match players
        match_players = get_match_players(match_id)

        #Now add players to output db:
        DB_FILE = output_db
        for match_player in match_players:
            player_id = match_player[1]
            operations.append(add_player(player_id))
        #add match
        operations.append(
            add_match(match_id, average_elo, map_id, patch_id, ladder_id, time))
        connect_and_modify_with_list(operations)
        DB_FILE = input_db

        #now get match_player actions
        for match_player in match_players:
            match_player_id_old, player_id, match_id, opening_id, civilization, victory, parser_version, time_parsed = match_player
            match_player_actions = get_match_player_actions(match_player_id_old)

            #now add info to real db
            DB_FILE = output_db
            add_unparsed_match_player(player_id, match_id, civilization,
                                      victory)
            match_player_id = get_match_player_id(player_id, match_id)
            generator = match_player_actions_generator_from_actions(
                match_player_actions, match_player_id)
            connect_and_modify_with_generator(generator)
            DB_FILE = input_db

    DB_FILE = output_db


def execute(input_folder, delete_replay_after_parse, analysis_only):
    #import a folder of replays, first add replays to db and then do analysis after
    if not analysis_only:
        for file in os.listdir(input_folder):
            file = os.path.join(input_folder, file)
            #important info in the map name
            match_id, player1_id, player2_id, average_elo, ladder_id = grab_replays_for_player.parse_filename(
                file)
            #match already in db!
            with open(file, 'rb') as data:
                success = parse_replay_file(match_id, player1_id, player2_id,
                                            average_elo, ladder_id, data)

            #Now delete file if flag was selected
            if success and delete_replay_after_parse:
                os.remove(file)

    #now do analytics
    items_needing_update = get_match_players_needing_update()
    completed_count = 0
    for match_player in items_needing_update:
        print(f'( {completed_count} / {len(items_needing_update)} )')
        #treat players opener regardless of opponent for this stage
        player_event_list = []
        events = []
        for action in get_actions_for_match_player(match_player[0]):
            events.append(
                aoe_replay_stats.Event(aoe_replay_stats.EventType(action[2]),
                                       action[3], None, action[4], action[5]))
        player_event_list.append(events)
        player_strategies = aoe_replay_stats.guess_strategy(player_event_list)
        #aoe_replay_stats.print_events(player_event_list, None, None, player_strategies)

        #now just update match_player
        update_match_player(player_strategies[0].value, match_player[0])
        completed_count += 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description="Parse replays in folder and store them in the DB")
    parser.add_argument("input",
                        help="Folder to draw replays from",
                        type=str,
                        default=0)
    parser.add_argument(
        "-a",
        "--analysis-only",
        help="Don't parse any new replays, just perform analysis",
        action='store_true')
    parser.add_argument(
        "-o",
        "--output-db",
        help="Point to a specific sqlite3 db to insert data into",
        type=str,
        default=DB_FILE)
    parser.add_argument(
        "-i",
        "--import-from-other-db",
        help="If set, will import all matches from given db into output db",
        type=str)
    parser.add_argument(
        "-X",
        "--delete-replay-after-parse",
        help="If set, this will delete replays after they have been parsed",
        action='store_true')

    args = parser.parse_args()
    DB_FILE = args.output_db
    init_db()
    update_schema()
    if args.import_from_other_db is not None:
        import_from_db(args.import_from_other_db)
    execute(args.input, args.delete_replay_after_parse, args.analysis_only)
