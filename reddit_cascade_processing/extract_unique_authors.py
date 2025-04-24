import argparse
import json
import logging
from tqdm import tqdm

def parse_args():
    parser = argparse.ArgumentParser(description="Extract unique authors from conversation JSONL files.")
    parser.add_argument("input_files", nargs="+", help="Input JSONL files (produced by extract_conversations.py).")
    parser.add_argument("-o", "--output", required=True, help="Output file path for newline-separated authors.")
    parser.add_argument("--loglevel", default="INFO", help="Logging level (DEBUG, INFO, WARNING, etc.)")
    return parser.parse_args()

def setup_logging(level):
    logging.basicConfig(
        level=getattr(logging, level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s"
    )

def extract_authors_from_file(filepath, author_set):
    with open(filepath, "r", encoding="utf-8") as f:
        for line in tqdm(f, desc=f"Processing {filepath}"):
            if not line.strip():
                continue
            try:
                data = json.loads(line.strip())
                for conv in data.values():
                    for author, _ in conv:
                        author_set.add(author)
            except Exception as e:
                logging.warning(f"Failed to parse line: {line[:80]}... ({str(e)})")

def main():
    args = parse_args()
    setup_logging(args.loglevel)

    authors = set()

    for input_file in args.input_files:
        extract_authors_from_file(input_file, authors)

    sorted_authors = sorted(authors)
    with open(args.output, "w", encoding="utf-8") as out_file:
        for author in sorted_authors:
            out_file.write(author + "\n")

    logging.info(f"Wrote {len(sorted_authors)} unique authors to {args.output}")

if __name__ == "__main__":
    main()
