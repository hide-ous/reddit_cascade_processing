import argparse
import json
import logging
import os
from collections import defaultdict
from tqdm import tqdm

def parse_args():
    parser = argparse.ArgumentParser(description="Extract Reddit conversations from JSONL files.")
    parser.add_argument("input_files", nargs="+", help="Input JSONL files containing Reddit comments or submissions.")
    parser.add_argument("-b", "--bots_file", default="bots.txt", help="Path to newline-separated list of bot usernames.")
    parser.add_argument("--dedup_authors", action="store_true", help="Keep only the first occurrence of each author per conversation.")
    parser.add_argument("--min_len", type=int, default=1, help="Minimum length of conversations to keep.")
    parser.add_argument("--max_len", type=int, default=1000000, help="Maximum length of conversations to keep.")
    parser.add_argument("-o", "--output", required=True, help="Output JSONL file path.")
    parser.add_argument("--loglevel", default="INFO", help="Logging level (e.g., DEBUG, INFO, WARNING).")
    return parser.parse_args()

def setup_logging(level):
    logging.basicConfig(
        level=getattr(logging, level.upper(), None),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_bots(bots_file):
    if not os.path.exists(bots_file):
        logging.warning(f"Bots file '{bots_file}' not found. No bots will be filtered.")
        return set()
    with open(bots_file, "r", encoding="utf-8") as f:
        return set(line.strip().lower() for line in f if line.strip())

def extract_conversation_id(entry):
    if "link_id" in entry:
        return entry["link_id"]
    elif "id" in entry:
        return entry["id"] if entry["id"].startswith("t3_") else "t3_" + entry["id"]
    return None

def is_valid_author(author):
    return author and author.strip().lower() not in {"[removed]", "[deleted]", ""}

def read_all_entries(filepaths):
    for filepath in filepaths:
        with open(filepath, "r", encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                try:
                    yield json.loads(line.strip())
                except json.JSONDecodeError:
                    logging.warning(f"Invalid JSON in {filepath}: {line.strip()[:80]}")
                    continue

def main():
    args = parse_args()
    setup_logging(args.loglevel)
    bots = load_bots(args.bots_file)

    conversations = defaultdict(list)

    logging.info("Processing input files...")
    total_lines = sum(1 for fp in args.input_files for _ in open(fp, 'r', encoding='utf-8'))
    entry_stream = tqdm(read_all_entries(args.input_files), total=total_lines, desc="Reading")

    for entry in entry_stream:
        author = entry.get("author", "")
        timestamp = entry.get("created_utc")
        conv_id = extract_conversation_id(entry)

        # Filter 1: Must have author and created_utc
        if author is None or timestamp is None:
            continue

        # Filter 2: Remove removed/deleted authors
        if not is_valid_author(author):
            continue

        author = author.strip().lower()

        conversations[conv_id].append((author, int(timestamp)))

    logging.info(f"Loaded {len(conversations)} raw conversations. Applying filters...")

    filtered_convs = {}
    for conv_id, turns in conversations.items():
        # Filter 3: Remove bot authors
        turns = [t for t in turns if t[0] not in bots]

        # Filter 4: Deduplicate authors
        if args.dedup_authors:
            seen = set()
            deduped = []
            for author, ts in turns:
                if author not in seen:
                    deduped.append((author, ts))
                    seen.add(author)
            turns = deduped

        # Sort by timestamp
        turns.sort(key=lambda x: x[1])

        # Filter 5: Length constraint
        if args.min_len <= len(turns) <= args.max_len:
            filtered_convs[conv_id] = turns

    logging.info(f"{len(filtered_convs)} conversations retained after filtering.")

    with open(args.output, "w", encoding="utf-8") as out_f:
        for conv_id, turns in filtered_convs.items():
            out_f.write(json.dumps({conv_id: turns}, ensure_ascii=False) + "\n")

    logging.info(f"Output written to {args.output}")

if __name__ == "__main__":
    main()
