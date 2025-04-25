import argparse
import json
import logging
import csv
from collections import defaultdict, Counter
from tqdm import tqdm
from itertools import combinations
import networkx as nx


def parse_args():
    parser = argparse.ArgumentParser(description="Filter cascades and build user-user subreddit co-occurrence network.")
    parser.add_argument("--cascades", required=True, help="Path to cascades file (JSONL).")
    parser.add_argument("--subreddit_counts", required=True, help="Path to author subreddit counts per year (JSONL).")
    parser.add_argument("--min_cascade_count", type=int, required=True, help="Minimum number of cascades a user must appear in to be included.")
    parser.add_argument("--min_cascade_size", type=int, default=5, help="Minimum cascade size.")
    parser.add_argument("--max_cascade_size", type=int, default=100, help="Maximum cascade size.")
    parser.add_argument("--min_subreddits", type=int, default=5, help="Minimum number of subreddits a user must appear in.")
    parser.add_argument("--year_start", type=int, default=None, help="Optional: Filter subreddit counts from this year.")
    parser.add_argument("--year_end", type=int, default=None, help="Optional: Filter subreddit counts up to this year.")
    parser.add_argument("--exclude_subreddits", help="Optional: Path to a file with subreddits to exclude (newline-separated).")
    parser.add_argument("--filtered_cascades_out", required=True, help="Output path for filtered cascades JSONL file.")
    parser.add_argument("--edge_list_out", required=True, help="Output path for CSV edge list.")
    parser.add_argument("--loglevel", default="INFO", help="Logging level (DEBUG, INFO, etc).")
    return parser.parse_args()


def setup_logging(level):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_exclude_list(path):
    if not path:
        return set()
    with open(path, "r", encoding="utf-8") as f:
        return set(line.strip().lower() for line in f if line.strip())


def load_cascades(path, min_size, max_size):
    cascades = []
    user_counts = Counter()
    with open(path, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Loading cascades"):
            obj = json.loads(line)
            for conv_id, user_time_list in obj.items():
                users = [user for user, _ in user_time_list]
                for user in set(users):
                    user_counts[user] += 1
                cascades.append((conv_id, user_time_list))
    return cascades, user_counts


def filter_cascades(cascades, valid_users, min_size, max_size):
    filtered = []
    for conv_id, user_time_list in cascades:
        new_list = [(u, t) for u, t in user_time_list if u in valid_users]
        if min_size <= len(new_list) <= max_size:
            filtered.append((conv_id, new_list))
    return filtered


def load_subreddit_counts(path, valid_users, year_start, year_end, exclude_subreddits, min_subreddits):
    user_subreddits = defaultdict(set)
    with open(path, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Loading subreddit counts"):
            obj = json.loads(line)
            user = obj['author']
            if user not in valid_users:
                continue
            for year, sub_dict in obj['data'].items():
                y = int(year)
                if year_start and y < year_start:
                    continue
                if year_end and y > year_end:
                    continue
                for sub, count in sub_dict.items():
                    if sub.lower() not in exclude_subreddits:
                        user_subreddits[user].add(sub)
    return {user: subs for user, subs in user_subreddits.items() if len(subs) >= min_subreddits}


def build_edge_list(user_subreddits):
    users = list(user_subreddits.keys())
    edges = defaultdict(int)
    for u1, u2 in tqdm(combinations(users, 2), desc="Computing edges", total=(len(users) * (len(users) - 1)) // 2):
        shared = user_subreddits[u1] & user_subreddits[u2]
        if shared:
            edges[(u1, u2)] = len(shared)
    return edges


def save_filtered_cascades(path, filtered_cascades):
    with open(path, "w", encoding="utf-8") as f:
        for conv_id, user_time_list in filtered_cascades:
            f.write(json.dumps({conv_id: user_time_list}, ensure_ascii=False) + "\n")


def save_edge_list(path, edges):
    with open(path, "w", encoding="utf-8", newline='') as f:
        writer = csv.writer(f)
        writer.writerow(["source", "target", "weight"])
        for (u1, u2), w in edges.items():
            writer.writerow([u1, u2, w])


def get_largest_connected_component(edges):
    G = nx.Graph()
    for (u1, u2), weight in edges.items():
        G.add_edge(u1, u2, weight=weight)
    largest_cc = max(nx.connected_components(G), key=len)
    logging.info(f"Largest connected component has {len(largest_cc)} users.")
    return largest_cc


def main():
    args = parse_args()
    setup_logging(args.loglevel)

    exclude_subreddits = load_exclude_list(args.exclude_subreddits)

    cascades, user_counts = load_cascades(
        args.cascades, args.min_cascade_size, args.max_cascade_size
    )

    initial_valid_users = {u for u, c in user_counts.items() if c >= args.min_cascade_count}

    user_subreddits = load_subreddit_counts(
        args.subreddit_counts, initial_valid_users, args.year_start, args.year_end, exclude_subreddits, args.min_subreddits
    )

    final_valid_users = set(user_subreddits.keys())

    filtered_cascades = filter_cascades(cascades, final_valid_users, args.min_cascade_size, args.max_cascade_size)

    edge_list = build_edge_list(user_subreddits)
    largest_cc_users = get_largest_connected_component(edge_list)

    final_users = final_valid_users & largest_cc_users

    filtered_cascades = filter_cascades(filtered_cascades, final_users, args.min_cascade_size, args.max_cascade_size)
    save_filtered_cascades(args.filtered_cascades_out, filtered_cascades)
    logging.info(f"Saved {len(filtered_cascades)} filtered cascades.")

    edge_list = {(u1, u2): w for (u1, u2), w in edge_list.items() if u1 in final_users and u2 in final_users}
    save_edge_list(args.edge_list_out, edge_list)
    logging.info(f"Saved edge list with {len(edge_list)} edges.")


if __name__ == "__main__":
    main()
