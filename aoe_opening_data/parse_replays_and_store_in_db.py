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
                            ladder_id integer DEFAULT 3,
                            patch_number integer DEFAULT 53347
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
    sql_commands.append(
        """CREATE INDEX IF NOT EXISTS idx_match_player_actions_match_player_id
                           on match_player_actions (match_player_id);""")

    sql_commands.append("""CREATE INDEX IF NOT EXISTS idx_match_player_match_id
                           on match_players (match_id);""")
    try:
        conn = sqlite3.connect(DB_FILE, timeout=60)
        c = conn.cursor()
        for sql_command in sql_commands:
            c.execute(sql_command)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()


def init_flat_db():
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
                            ladder_id integer DEFAULT 3,
                            patch_number integer NOT NULL,
                            player1_id integer NOT NULL,
                            player1_opening_flag0 bool NOT NULL,
                            player1_opening_flag1 bool NOT NULL,
                            player1_opening_flag2 bool NOT NULL,
                            player1_opening_flag3 bool NOT NULL,
                            player1_opening_flag4 bool NOT NULL,
                            player1_opening_flag5 bool NOT NULL,
                            player1_opening_flag6 bool NOT NULL,
                            player1_opening_flag7 bool NOT NULL,
                            player1_opening_flag8 bool NOT NULL,
                            player1_opening_flag9 bool NOT NULL,
                            player1_opening_flag10 bool NOT NULL,
                            player1_opening_flag11 bool NOT NULL,
                            player1_opening_flag12 bool NOT NULL,
                            player1_opening_flag13 bool NOT NULL,
                            player1_opening_flag14 bool NOT NULL,
                            player1_opening_flag15 bool NOT NULL,
                            player1_opening_flag16 bool NOT NULL,
                            player1_opening_flag17 bool NOT NULL,
                            player1_opening_flag18 bool NOT NULL,
                            player1_opening_flag19 bool NOT NULL,
                            player1_opening_flag20 bool NOT NULL,
                            player1_opening_flag21 bool NOT NULL,
                            player1_opening_flag22 bool NOT NULL,
                            player1_opening_flag23 bool NOT NULL,
                            player1_opening_flag24 bool NOT NULL,
                            player1_opening_flag25 bool NOT NULL,
                            player1_opening_flag26 bool NOT NULL,
                            player1_opening_flag27 bool NOT NULL,
                            player1_opening_flag28 bool NOT NULL,
                            player1_opening_flag29 bool NOT NULL,
                            player1_opening_flag30 bool NOT NULL,
                            player1_opening_flag31 bool NOT NULL,
                            player1_civilization integer NOT NULL,
                            player1_victory integer NOT NULL,
                            player1_parser_version integer NOT NULL,
                            player2_id integer NOT NULL,
                            player2_opening_flag0 bool NOT NULL,
                            player2_opening_flag1 bool NOT NULL,
                            player2_opening_flag2 bool NOT NULL,
                            player2_opening_flag3 bool NOT NULL,
                            player2_opening_flag4 bool NOT NULL,
                            player2_opening_flag5 bool NOT NULL,
                            player2_opening_flag6 bool NOT NULL,
                            player2_opening_flag7 bool NOT NULL,
                            player2_opening_flag8 bool NOT NULL,
                            player2_opening_flag9 bool NOT NULL,
                            player2_opening_flag10 bool NOT NULL,
                            player2_opening_flag11 bool NOT NULL,
                            player2_opening_flag12 bool NOT NULL,
                            player2_opening_flag13 bool NOT NULL,
                            player2_opening_flag14 bool NOT NULL,
                            player2_opening_flag15 bool NOT NULL,
                            player2_opening_flag16 bool NOT NULL,
                            player2_opening_flag17 bool NOT NULL,
                            player2_opening_flag18 bool NOT NULL,
                            player2_opening_flag19 bool NOT NULL,
                            player2_opening_flag20 bool NOT NULL,
                            player2_opening_flag21 bool NOT NULL,
                            player2_opening_flag22 bool NOT NULL,
                            player2_opening_flag23 bool NOT NULL,
                            player2_opening_flag24 bool NOT NULL,
                            player2_opening_flag25 bool NOT NULL,
                            player2_opening_flag26 bool NOT NULL,
                            player2_opening_flag27 bool NOT NULL,
                            player2_opening_flag28 bool NOT NULL,
                            player2_opening_flag29 bool NOT NULL,
                            player2_opening_flag30 bool NOT NULL,
                            player2_opening_flag31 bool NOT NULL,
                            player2_civilization integer NOT NULL,
                            player2_victory integer NOT NULL,
                            player2_parser_version integer NOT NULL,
                            CONSTRAINT fk_player FOREIGN KEY(player1_id) REFERENCES players(id) ON DELETE CASCADE,
                            CONSTRAINT fk_player FOREIGN KEY(player2_id) REFERENCES players(id) ON DELETE CASCADE
                            ) WITHOUT ROWID; """)

    sql_commands.append(""" CREATE TABLE IF NOT EXISTS players (
                            id integer NOT NULL PRIMARY KEY,
                            name text
                            ) WITHOUT ROWID; """)

    sql_commands.append(""" CREATE TABLE IF NOT EXISTS match_player_actions (
                            id integer NOT NULL PRIMARY KEY,
                            match_id integer NOT NULL,
                            player_id integer NOT NULL,
                            event_type integer NOT NULL,
                            event_id integer NOT NULL,
                            time integer NOT NULL,
                            duration integer NOT NULL,
                            CONSTRAINT fk_match_id FOREIGN KEY(match_id) REFERENCES matches(id) ON DELETE CASCADE,
                            CONSTRAINT fk_player_id FOREIGN KEY(player_id) REFERENCES players(id) ON DELETE CASCADE
                            ); """)
    try:
        conn = sqlite3.connect(DB_FILE, timeout=60)
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

    # Second update to db, completely renaming the opening flags
    test = connect_and_return("""SELECT * from openings where id = 5;""", ())
    if test is not None and len(test) and len(
            test[0]) and test[0][1] == "PostmillDrushFlush":
        # Clear table and insert new openings
        connect_and_modify("""DELETE FROM openings""", ())
        init_db()  # Cheap way to rebuild table
    #third update add patch id
    connect_and_modify(
        """ALTER TABLE matches ADD COLUMN patch_number integer DEFAULT 53347;""", ())
    #fourth update, try a time index
    connect_and_modify(
        """CREATE INDEX time_index ON matches(time)""", ())


### UNIVERSAL SQL FUNCTIONS ###
def connect_and_modify(statement):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=60)
        c = conn.cursor()
        c.execute(statement)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()


def connect_and_modify(statement, args):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=60)
        c = conn.cursor()
        c.execute(statement, args)
        conn.commit()
    except Exception as e:
        print(e)
    finally:
        conn.close()


def connect_and_modify_with_generator(generator):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=60)
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
        conn = sqlite3.connect(DB_FILE, timeout=60)
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


def connect_and_return_with_list(operations):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=60)
        c = conn.cursor()
        return_list = []
        for statement, args in operations:
            c.execute(statement, args)
            return_list.append(c.fetchall())
        return return_list
    except Exception as e:
        print(e)
    finally:
        conn.close()


def connect_and_modify_with_list(operations):
    try:
        conn = sqlite3.connect(DB_FILE, timeout=60)
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


def add_match(match_id, average_elo, map_id, patch_id, ladder_id, patch_number, time=False):
    if time:
        return (
            "INSERT OR IGNORE INTO matches(id, average_elo, map_id, patch_id, ladder_id, time, patch_number) VALUES(?,?,?,?,?,?,?)",
            (match_id, average_elo, map_id, patch_id, ladder_id, time, patch_number))
    else:
        return (
            "INSERT OR IGNORE INTO matches(id, average_elo, map_id, patch_id, ladder_id, patch_number) VALUES(?,?,?,?,?,?)",
            (match_id, average_elo, map_id, patch_id, ladder_id, patch_number))


def get_matches():
    matches = connect_and_return("SELECT * FROM matches", ())
    if len(matches) == 0:
        return None
    return matches


def update_match_player(opening_id, match_player_id):
    return ("""UPDATE match_players
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


def flat_match_player_actions_generator_from_actions(match_player_actions,
                                                match_id,
                                                player_id):
    statement = """INSERT OR IGNORE INTO match_player_actions
                   (match_id, player_id, event_type, event_id, time, duration)
                   VALUES (?,?,?,?,?,?)"""
    for id_old, match_player_id_old, event_type, event_id, time, duration in match_player_actions:
        if event_type != aoe_replay_stats.EventType.TECH.value:
            continue
        yield statement, (match_id, player_id, event_type, event_id, time, duration)

def match_player_actions_generator_from_actions(match_player_actions,
                                                match_player_id,
                                                minimal_import):
    statement = """INSERT OR IGNORE INTO match_player_actions
                   (match_player_id, event_type, event_id, time, duration)
                   VALUES (?,?,?,?,?)"""
    for id_old, match_player_id_old, event_type, event_id, time, duration in match_player_actions:
        if minimal_import:
            if event_type != aoe_replay_stats.EventType.TECH.value:
                continue
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
    if match is None or len(match) == 0:
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
    if match_player_id is None or len(match_player_id) == 0:
        return None
    return match_player_id[0][0]


def get_match_players_needing_update():
    #Added small thing to ignore first 1m matches when searching, remove this if you need a whole db update
    match_players = connect_and_return(
        "SELECT * FROM match_players WHERE parser_version < ? AND match_id > 202400210",
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


def get_actions_for_match_players(match_player_list):
    operations = []
    for match_player in match_player_list:
        operations.append(
            ("SELECT * FROM match_player_actions WHERE match_player_id = ?",
             (match_player[0],)))
    match_players_actions = connect_and_return_with_list(operations)
    if len(match_players_actions) == 0:
        return None
    return match_players_actions


def parse_replay_file(match_id, player1_id, player2_id, average_elo, ladder_id,
                      file_data, patch_number):
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
                  header.save_version, ladder_id, patch_number))

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


def import_from_db(input_db, minimal_import, flat_import):
    # Since we are using a global DB handle im going to do some weird hacky stuff to not rewrite routines
    global DB_FILE
    if minimal_import:
        print(f'Writing minimal_import from {input_db} to {DB_FILE}')
    elif flat_import:
        print(f'Writing flat_import from {input_db} to {DB_FILE}')
    else:
        print(f'Writing from {input_db} to {DB_FILE}')

    output_conn = sqlite3.connect(DB_FILE, timeout=60)
    output_cursor = output_conn.cursor()
    input_conn = sqlite3.connect(input_db, timeout=60)
    input_cursor = input_conn.cursor()

    #first get all matches
    input_cursor.execute(
        """SELECT m.id, m.average_elo, m.map_id, m.time, m.patch_id, m.ladder_id, m.patch_number from matches m
                            JOIN match_players a on a.match_id = m.id
                            JOIN match_players b on b.match_id = m.id
                            WHERE a.id != b.id
                              AND a.parser_version == ? 
                              AND b.parser_version == ?
                              AND a.victory = 1""",
        (aoe_replay_stats.PARSER_VERSION, aoe_replay_stats.PARSER_VERSION))
    matches = input_cursor.fetchall()

    #now iterate through matches and add them to other db
    i = 0
    for match in matches:
        if i % 50 == 0:  #throttle for speed reasons
            print(f'( {i} / {len(matches)} )')
        i += 1
        match_id, average_elo, map_id, time, patch_id, ladder_id, patch_number = match
        #Set patch to invalid if its unavailable
        if patch_number is None:
          patch_number = -1

        #Check if match_id exists in output db
        output_cursor.execute("SELECT * FROM matches WHERE id=?", (match_id,))
        match = output_cursor.fetchall()
        if not match is None and len(match):
            continue

        #now get match players
        input_cursor.execute("SELECT * FROM match_players WHERE match_id = ?",
                             (match_id,))
        match_players = input_cursor.fetchall()

        #Now add players to output db:
        for match_player in match_players:
            player_id = match_player[1]
            output_cursor.execute("INSERT OR IGNORE INTO players(id) VALUES(?)",
                                  (player_id,))
        if flat_import:
            if len(match_players) != 2:
                print(f'Match ({match_id}): only has {len(match_players)} player(s)')
                return
            statement = "INSERT INTO matches(id, average_elo, map_id, patch_id, ladder_id, time, patch_number,"
            for j in range(1,len(match_players)+1):
              statement += f' player{j}_id,'
              for k in range(32) : #number of flags
                statement += f' player{j}_opening_flag{k},'
              statement += f' player{j}_civilization,'
              statement += f' player{j}_victory,'
              statement += f' player{j}_parser_version,'
            statement = statement[:-1] #remove trailing comma
            statement += ") "
            statement +="VALUES(?,?,?,?,?,?,?,"
            for j in range(1,len(match_players)+1):
              statement += f'?,'
              for k in range(32) : #number of flags
                statement += f'?,'
              statement += f'?,'
              statement += f'?,'
              statement += f'?,'
            statement = statement[:-1] #remove trailing comma
            statement+= ");"

            #now generate tuple
            arguments = []
            arguments.append(match_id)
            arguments.append(average_elo)
            arguments.append(map_id)
            arguments.append(patch_id)
            arguments.append(ladder_id)
            arguments.append(time)
            arguments.append(patch_number)
            for j in range(len(match_players)):
              arguments.append(match_players[j][1])
              for k in range(32) : #number of flags
                arguments.append((match_players[j][3] & 2**k) != 0)
              arguments.append(match_players[j][4])
              arguments.append(match_players[j][5])
              arguments.append(match_players[j][6])

            output_cursor.execute(statement, tuple(arguments))

            #now do match player actions
            for match_player in match_players:
              match_player_id_old, player_id, match_id, opening_id, civilization, victory, parser_version, time_parsed = match_player
              input_cursor.execute(
                  "SELECT * FROM match_player_actions WHERE match_player_id = ?",
                  (match_player_id_old,))
              match_player_actions = input_cursor.fetchall()
              for statement, args in flat_match_player_actions_generator_from_actions(
                      match_player_actions, match_id, player_id):
                  output_cursor.execute(statement, args)
        else:
            #add match
            output_cursor.execute(
                "INSERT OR IGNORE INTO matches(id, average_elo, map_id, patch_id, ladder_id, time, patch_number) VALUES(?,?,?,?,?,?,?)",
                (match_id, average_elo, map_id, patch_id, ladder_id, time, patch_number))

            #now get match_player actions
            for match_player in match_players:
                match_player_id_old, player_id, match_id, opening_id, civilization, victory, parser_version, time_parsed = match_player
                input_cursor.execute(
                    "SELECT * FROM match_player_actions WHERE match_player_id = ?",
                    (match_player_id_old,))
                match_player_actions = input_cursor.fetchall()

                #now add info to real db
                if minimal_import:
                    output_cursor.execute(
                        """INSERT OR IGNORE INTO match_players(player_id, match_id, opening_id, civilization, victory, parser_version, time_parsed) VALUES
                                          (?,?,?,?,?,?,?)""",
                        (player_id, match_id, opening_id, civilization, victory,
                         parser_version, time_parsed))
                else:
                    output_cursor.execute(
                        """INSERT OR IGNORE INTO match_players(player_id, match_id, civilization, victory) VALUES
                                          (?,?,?,?)""",
                        (player_id, match_id, civilization, victory))

                match_player_id = output_cursor.lastrowid

                for statement, args in match_player_actions_generator_from_actions(
                        match_player_actions, match_player_id, minimal_import):
                    output_cursor.execute(statement, args)
    output_conn.commit()
    output_conn.close()
    input_conn.close()


def slice_generator(input_list, slice_length):
    for i in range(0, len(input_list), slice_length):
        yield input_list[i:i + slice_length]


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
    slice_size = 25
    operations = []
    #convert to slices to do operations in bulk
    for match_players in slice_generator(items_needing_update, slice_size):
        print(f'( {completed_count} / {len(items_needing_update)} )')
        #treat players opener regardless of opponent for this stage
        match_players_actions = get_actions_for_match_players(match_players)
        index = -1
        for actions in match_players_actions:
            player_event_list = []
            events = []
            index += 1
            if actions is None:
                continue
            for action in actions:
                events.append(
                    aoe_replay_stats.Event(
                        aoe_replay_stats.EventType(action[2]), action[3], None,
                        action[4], action[5]))
            player_event_list.append(events)
            player_strategies = aoe_replay_stats.guess_strategy(
                player_event_list)

            #now just update match_player
            if not player_strategies:
              continue
            operations.append(
                update_match_player(player_strategies[0],
                                    match_players[index][0]))
        if len(operations) > 100:
            connect_and_modify_with_list(operations)
            operations = []
        completed_count += slice_size
    if len(operations):
        connect_and_modify_with_list(operations)


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
        "-m",
        "--minimal-import-from-other-db",
        help="If set, will import minimal data from given db into output db",
        type=str)
    parser.add_argument(
        "-f",
        "--flat-import-from-other-db",
        help="If set, will import minimal data from given db into output flat db",
        type=str)
    parser.add_argument(
        "-X",
        "--delete-replay-after-parse",
        help="If set, this will delete replays after they have been parsed",
        action='store_true')

    args = parser.parse_args()
    DB_FILE = args.output_db
    minimal_import = False
    flat_import = False
    if args.minimal_import_from_other_db is not None:
        args.import_from_other_db = args.minimal_import_from_other_db
        minimal_import = True
    elif args.flat_import_from_other_db is not None:
        args.import_from_other_db = args.flat_import_from_other_db
        flat_import = True
    if flat_import:
      init_flat_db()
    else:
      init_db()
      update_schema()
      
    if args.import_from_other_db is not None:
        import_from_db(args.import_from_other_db, minimal_import, flat_import)
    execute(args.input, args.delete_replay_after_parse, args.analysis_only)
