import argparse
import json
import logging
import os
import zstandard as zstd
import multiprocessing as mp
from collections import defaultdict
from datetime import datetime
from tqdm import tqdm

def parse_args():
    parser = argparse.ArgumentParser(description="Count Reddit contributions by author/year/subreddit.")
    parser.add_argument("authors_file", help="Path to file with list of authors (newline-separated).")
    parser.add_argument("data_path", help="Path to a directory containing 'comments/' and 'submissions/' folders.")
    parser.add_argument("-o", "--output", required=True, help="Output JSONL file path.")
    parser.add_argument("-w", "--workers", type=int, default=4, help="Number of parallel worker processes.")
    parser.add_argument("--loglevel", default="INFO", help="Logging level (DEBUG, INFO, etc).")
    return parser.parse_args()

def setup_logging(level):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def load_authors(filepath):
    with open(filepath, "r", encoding="utf-8") as f:
        return set(line.strip().lower() for line in f if line.strip())

# Replacement for lambda-in-defaultdict (needs to be top-level for multiprocessing)
def nested_dict():
    return defaultdict(dict)

def deeper_nested_dict():
    return defaultdict(lambda: defaultdict(int))

def process_file(args):
    filepath, authors = args
    logging.info(f"Parsing {filepath}")
    author_data = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    try:
        with open(filepath, 'rb') as f:
            dctx = zstd.ZstdDecompressor(max_window_size=2 ** 31)
            reader = dctx.stream_reader(f)
            buffer = ''
            while True:
                chunk = reader.read(2**27)
                if not chunk:
                    break
                chunk = buffer + chunk.decode('utf-8', errors='ignore')
                lines = chunk.split('\n')
                buffer = lines[-1]

                for line in lines[:-1]:
                    if not line.strip():
                        continue
                    try:
                        obj = json.loads(line)
                        author = obj.get('author', '').lower()
                        subreddit = obj.get('subreddit')
                        created_utc = obj.get('created_utc')

                        if author not in authors or not subreddit or created_utc is None:
                            continue

                        year = str(datetime.utcfromtimestamp(int(created_utc)).year)
                        author_data[author][year][subreddit] += 1
                    except Exception as e:
                        logging.debug(f"Skipping line due to error: {e}")
    except Exception as e:
        logging.warning(f"Failed to process {filepath}: {e}")

    # Convert to regular dicts before sending back to avoid pickling issues
    return json.loads(json.dumps(author_data))

def merge_results(results):
    merged = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for result in results:
        for author, years in result.items():
            for year, subreddits in years.items():
                for subreddit, count in subreddits.items():
                    merged[author][year][subreddit] += count
    return merged

def find_zst_files(data_path):
    files = []
    for subdir in ["comments", "submissions"]:
        full_path = os.path.join(data_path, subdir)
        if os.path.isdir(full_path):
            for fname in os.listdir(full_path):
                if fname.endswith(".zst"):
                    files.append(os.path.join(full_path, fname))
    return files

def write_jsonl(data, output_path):
    with open(output_path, "w", encoding="utf-8") as f:
        for author, yearly_data in data.items():
            entry = {
                "author": author,
                "data": yearly_data
            }
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

def main():
    args = parse_args()
    setup_logging(args.loglevel)

    logging.info("Loading author list...")
    authors = load_authors(args.authors_file)
    logging.info(f"Loaded {len(authors)} authors.")

    logging.info("Searching for .zst files...")
    zst_files = find_zst_files(args.data_path)
    logging.info(f"Found {len(zst_files)} files to process.")

    pool_inputs = [(f, authors) for f in zst_files]

    with mp.Pool(processes=args.workers) as pool:
        results = list(tqdm(pool.imap_unordered(process_file, pool_inputs), total=len(zst_files), desc="Processing files"))

    logging.info("Merging results...")
    merged = merge_results(results)

    logging.info(f"Writing JSONL output to {args.output}")
    write_jsonl(merged, args.output)

    logging.info("Done.")

if __name__ == "__main__":
    # python -u reddit_cascade_processing/count_author_contributions.py data/interim/authors.txt data/external/Reddit/zst_drops/ --workers=30 -o=data/interim/author_subreddits.jsonl
    main()
