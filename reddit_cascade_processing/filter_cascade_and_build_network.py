import argparse
import json
import logging
import csv
from collections import defaultdict, Counter
from datetime import datetime, timezone

from tqdm import tqdm
from itertools import combinations, islice
import networkx as nx
from multiprocessing import Pool, cpu_count


def parse_args():
    parser = argparse.ArgumentParser(description="Filter cascades and build user-user subreddit co-occurrence network.")
    parser.add_argument("--cascades", required=True, help="Path to cascades file (JSONL).")
    parser.add_argument("--subreddit_counts", required=True, help="Path to author subreddit counts per year (JSONL).")
    parser.add_argument("--min_cascade_count", type=int, required=True, help="Minimum number of cascades a user must appear in to be included.")
    parser.add_argument("--min_cascade_size", type=int, default=5, help="Minimum cascade size.")
    parser.add_argument("--max_cascade_size", type=int, default=100, help="Maximum cascade size.")
    parser.add_argument("--min_subreddits", type=int, default=1, help="Minimum number of subreddits a user must appear in.")
    parser.add_argument("--year_start", type=int, default=None, help="Optional: Filter subreddit counts from this year.")
    parser.add_argument("--year_end", type=int, default=None, help="Optional: Filter subreddit counts up to this year.")
    parser.add_argument("--exclude_subreddits", nargs="*", help="Optional: Subreddits to exclude.")
    parser.add_argument("--filtered_cascades_out", required=True, help="Output path for filtered cascades JSONL file.")
    parser.add_argument("--edge_list_out", required=True, help="Output path for CSV edge list.")
    parser.add_argument("--loglevel", default="INFO", help="Logging level (DEBUG, INFO, etc).")
    return parser.parse_args()


def setup_logging(level):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )


def load_exclude_list(subreddits):
    return set(sub.lower() for sub in subreddits) if subreddits else set()


def process_conversations(path, start_timestamp=None, end_timestamp=None):
    """
    Processes conversations from a file, applying date filtering.

    Args:
        path (str): Path to the JSONL file.
        start_timestamp (float, optional): Start timestamp for filtering. Defaults to None.
        end_timestamp (float, optional): End timestamp for filtering. Defaults to None.

    Returns:
        list: A list of (conversation_id, filtered_user_time_list) tuples.
        Counter: A Counter object containing user counts from the filtered conversations.
    """
    processed_cascades = []
    user_counts = Counter()
    with open(path, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Processing conversations"):
            obj = json.loads(line)
            for conv_id, user_time_list in obj.items():
                if start_timestamp or end_timestamp:
                    filtered_user_time_list = [(user, time) for user, time in user_time_list
                                                if (start_timestamp is None or time >= start_timestamp) and
                                                   (end_timestamp is None or time <= end_timestamp)]
                    if not filtered_user_time_list:
                        continue
                    processed_cascades.append((conv_id, filtered_user_time_list))
                    for user, _ in filtered_user_time_list:
                        user_counts[user] += 1
                else:
                    processed_cascades.append((conv_id, user_time_list))
                    for user, _ in user_time_list:
                        user_counts[user] += 1
    return processed_cascades, user_counts


def process_conversations(path, start_timestamp=None, end_timestamp=None):
    """
    Processes conversations from a file, applying UTC date filtering.

    Args:
        path (str): Path to the JSONL file.
        start_timestamp (float, optional): Start UTC timestamp for filtering. Defaults to None.
        end_timestamp (float, optional): End UTC timestamp for filtering. Defaults to None.

    Returns:
        list: A list of (conversation_id, filtered_user_time_list) tuples.
        Counter: A Counter object containing user counts from the filtered conversations.
    """
    processed_cascades = []
    user_counts = Counter()
    with open(path, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc="Processing conversations"):
            obj = json.loads(line)
            for conv_id, user_time_list in obj.items():
                if start_timestamp or end_timestamp:
                    filtered_user_time_list = [(user, time) for user, time in user_time_list
                                                if (start_timestamp is None or time >= start_timestamp) and
                                                   (end_timestamp is None or time <= end_timestamp)]
                    if not filtered_user_time_list:
                        continue
                    processed_cascades.append((conv_id, filtered_user_time_list))
                    for user, _ in filtered_user_time_list:
                        user_counts[user] += 1
                else:
                    processed_cascades.append((conv_id, user_time_list))
                    for user, _ in user_time_list:
                        user_counts[user] += 1
    return processed_cascades, user_counts

def load_cascades(path, min_size, max_size, start_date=None, end_date=None):
    """
    Loads cascades from a file, optionally filtering by UTC date and returning user counts
    based on the same filter.

    Args:
        path (str): Path to the JSONL file.
        min_size (int): Minimum size of the cascade (not currently used in the provided code).
        max_size (int): Maximum size of the cascade (not currently used in the provided code).
        start_date (datetime.datetime, optional): Start UTC date for filtering. Defaults to None.
        end_date (datetime.datetime, optional): End UTC date for filtering. Defaults to None.

    Returns:
        list: A list of (conversation_id, user_time_list) tuples for cascades within the size limits and UTC date range.
        Counter: A Counter object containing counts of users who participated in conversations
                 within the specified UTC date range.
    """
    start_timestamp = datetime.timestamp(start_date.astimezone(timezone.utc)) if start_date else None
    end_timestamp = datetime.timestamp(end_date.astimezone(timezone.utc)) if end_date else None

    filtered_cascades, filtered_user_counts = process_conversations(path, start_timestamp, end_timestamp)

    return filtered_cascades, filtered_user_counts

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


def chunked_iterable(iterable, size):
    iterator = iter(iterable)
    while True:
        chunk = list(islice(iterator, size))
        if not chunk:
            break
        yield chunk


def compute_edge_batch(args):
    user_subreddits, pair_batch = args
    edges = defaultdict(int)
    for u1, u2 in pair_batch:
        shared = user_subreddits[u1] & user_subreddits[u2]
        if shared:
            edges[(u1, u2)] = len(shared)
    return edges


def build_edge_list(user_subreddits):
    users = list(user_subreddits.keys())
    user_pairs = combinations(users, 2)
    all_edges = defaultdict(int)
    with Pool(cpu_count()) as pool:
        for batch_result in tqdm(pool.imap_unordered(
                compute_edge_batch,
                map(lambda x: (user_subreddits, x), chunked_iterable(user_pairs, 1000000))),
                desc="Computing edges"):
            for key, val in batch_result.items():
                all_edges[key] += val
    return all_edges


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

    start_date = datetime(2023, 1, 1)  # Example start date
    end_date = datetime(2023, 12, 31)  # Example end date

    cascades, user_counts = load_cascades(
        args.cascades, args.min_cascade_size, args.max_cascade_size, start_date=start_date, end_date=end_date
    )

    initial_valid_users = {u for u, c in user_counts.items() if c >= args.min_cascade_count}

    user_subreddits = load_subreddit_counts(
        args.subreddit_counts, initial_valid_users, args.year_start, args.year_end, exclude_subreddits, args.min_subreddits
    )

    final_valid_users = set(user_subreddits.keys())
    logging.info(f"Final valid users: {len(final_valid_users)}")

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
