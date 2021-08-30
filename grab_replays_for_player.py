import requests
import sys
import zipfile
import os
import io
import time
import re
import argparse

import parse_replays_and_store_in_db


def parse_filename(fname):
    head, tail = os.path.split(fname)
    print(tail)
    m = re.search(r'([^_]+)_([^_]+)_vs_([^_]+)-([^_.]+)\(([^_.]+)\)', tail)
    if m:
        match_id = m.group(1)
        player1_id = m.group(2)
        player2_id = m.group(3)
        average_elo = m.group(4)
        ladder = m.group(5)
    else:
        m = re.search(r'([^_]+)_([^_]+)_vs_([^_]+)-([^_.]+)', tail)
        if m:
            match_id = m.group(1)
            player1_id = m.group(2)
            player2_id = m.group(3)
            average_elo = m.group(4)
            ladder = 3
    return match_id, player1_id, player2_id, average_elo, ladder


def execute(minimum_elo, maximum_elo, output_folder, player_id, add_to_db):
    if player_id:
        #specific player
        matches = requests.get(
            f"https://aoe2.net/api/player/matches?game=aoe2de&profile_id={player_id}&count=1000"
        )
    else:
        #get most recent 1000 matching with a 2 hour delay to ensure that the replays had time to get set
        search_time = round(time.time_ns() / 1000000000) - 2 * 60 * 60

        matches = requests.get(
            f"https://aoe2.net/api/matches?game=aoe2de&count=1000&since={search_time}"
        )
        print(matches.url)

    matches = matches.json()

    if not add_to_db:
        path = output_folder
        if not os.path.exists(path):
            os.mkdir(path)

    for match in matches:
        if not match["ranked"]:
            continue
        #1v1 rm and empire wars only
        if match["leaderboard_id"] != 3 and match["leaderboard_id"] != 13:
            continue
        match_id = match["match_id"]
        average_rating = 0
        divisor = 0
        for player in match["players"]:
            if player["rating"] is not None:
                average_rating += player["rating"]
                divisor += 1
        if divisor:
            average_rating = round(average_rating / divisor)
        if average_rating < minimum_elo or average_rating > maximum_elo:
            continue

        #if file already exists go to next game, dont want to download games we already have
        replay_name = f'{match_id}_{match["players"][0]["profile_id"]}_vs_{match["players"][1]["profile_id"]}-{average_rating}({match["leaderboard_id"]}).aoe2record'
        if os.path.exists(replay_name):
            continue

        for player in match["players"]:
            r = requests.get(
                f"https://aoe.ms/replay/?gameId={match_id}&profileId={player['profile_id']}"
            )
            print(r.url)
            print(r.status_code)
            if r.status_code != 404:
                #we found a match! Don't find another for this game.
                #now unzip it
                replay_zip = zipfile.ZipFile(io.BytesIO(r.content))
                replay = replay_zip.read(replay_zip.namelist()[0])
                if add_to_db:
                    #Write directly to db!
                    parse_replays_and_store_in_db.parse_replay_file(
                        match_id, match["players"][0]["profile_id"],
                        match["players"][1]["profile_id"], average_rating,
                        match["leaderboard_id"], io.BytesIO(replay))
                else:
                    with open(os.path.join(path, replay_name), 'wb') as f:
                        f.write(replay)
                break


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
    parser.add_argument("-o",
                        "--output-folder",
                        help="Folder to save recs to",
                        type=str,
                        default="All Matches")
    parser.add_argument("-a",
                        "--add-to-db",
                        help="Add files directly to db, don't save locally",
                        action='store_true')
    parser.add_argument(
        "-r",
        "--repeat",
        help="Run next query when first query finishes... forever and ever",
        action='store_true')
    parser.add_argument(
        "-i",
        "--player-id",
        help=
        "[OPTIONAL] Id for single player to fetch data for - defaults to all players",
        type=int,
        default=0)

    args = parser.parse_args()
    #make sure db exists and is updated!
    if args.add_to_db:
        parse_replays_and_store_in_db.init_db()
        parse_replays_and_store_in_db.update_schema()
    if args.repeat:
        while True:
            execute(args.minimum_elo, args.maximum_elo, args.output_folder,
                    args.player_id, args.add_to_db)
    else:
        execute(args.minimum_elo, args.maximum_elo, args.output_folder,
                args.player_id, args.add_to_db)
