import argparse
import json
import zstandard
import datetime
import logging
import re
import multiprocessing as mp

CHUNK_SIZE = 2 ** 27
MAX_WINDOW_SIZE = (2 ** 29) * 2
JSON_DECODER = json.JSONDecoder()
BATCH_SIZE = 1000  # Lines per process

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--fields', nargs='+', help='Only keep these fields from each comment (if present).')
    parser.add_argument('-o', '--output', help='Output filename. Defaults to filtered_comments_<timestamp>.json')
    parser.add_argument('file', help='The zst file to process.')
    parser.add_argument('-u', '--user', help='Filter comments by this user.')
    parser.add_argument('-s', '--subreddit', help='Filter comments from this subreddit.')
    date_group = parser.add_mutually_exclusive_group()
    date_group.add_argument('-d', '--date', help='Filter comments from this date (format: YYYY-MM-DD).')
    date_group.add_argument('-dr', '--date_range', nargs=2,
                            help='Filter comments within this date range (format: YYYY-MM-DD YYYY-MM-DD).')
    parser.add_argument('-c', '--comment_only', help='Only display comment text, not metadata.', action='store_true')
    parser.add_argument('-k', '--keyword', help='Search for a keyword or phrase within the comments.')
    parser.add_argument('-l', '--link', help='Display the link to the comment when used with --comment_only.',
                        action='store_true')
    return parser.parse_args()

def read_lines_zst(file_name):
    with open(file_name, 'rb') as file_handle:
        buffer = ''
        reader = zstandard.ZstdDecompressor(max_window_size=MAX_WINDOW_SIZE).stream_reader(file_handle)
        while True:
            chunk = reader.read(CHUNK_SIZE)
            if not chunk:
                break
            lines = (buffer + chunk.decode(errors='ignore')).split("\n")
            for line in lines[:-1]:
                yield line.strip()
            buffer = lines[-1]
        reader.close()

def init_filter(args):
    global FILTER_CONFIG
    date = None
    date_range_start = None
    date_range_end = None

    if args.date:
        date = datetime.datetime.strptime(args.date, '%Y-%m-%d').date()
    if args.date_range:
        date_range_start = datetime.datetime.strptime(args.date_range[0], '%Y-%m-%d').date()
        date_range_end = datetime.datetime.strptime(args.date_range[1], '%Y-%m-%d').date()

    FILTER_CONFIG = {
        "user": args.user,
        "subreddit": args.subreddit,
        "date": date,
        "date_range": (date_range_start, date_range_end),
        "keyword": args.keyword.lower() if args.keyword else None,
        "comment_only": args.comment_only,
        "link": args.link,
        "fields": args.fields

    }

def filter_comments(batch):
    results = []
    for line in batch:
        try:
            comment = JSON_DECODER.raw_decode(line)[0]

            # if comment.get('author', '').lower() == 'automoderator' or comment.get('body') in ['[deleted]', '[removed]']:
            #     continue

            if FILTER_CONFIG['user']:
                pattern = re.escape(FILTER_CONFIG['user']).replace(r'\*', '.*')
                if not re.fullmatch(pattern, comment.get('author', ''), re.IGNORECASE):
                    continue

            if FILTER_CONFIG['subreddit'] and comment.get('subreddit') != FILTER_CONFIG['subreddit']:
                continue

            comment_date = datetime.datetime.utcfromtimestamp(int(comment['created_utc'])).date()
            if FILTER_CONFIG['date'] and comment_date != FILTER_CONFIG['date']:
                continue
            start, end = FILTER_CONFIG['date_range']
            if start and end and (comment_date < start or comment_date > end):
                continue

            if FILTER_CONFIG['keyword'] and FILTER_CONFIG['keyword'] not in comment.get('body', '').lower():
                continue

            if FILTER_CONFIG['comment_only']:
                comment_str = comment['body']
                if FILTER_CONFIG['link']:
                    permalink = comment.get('permalink')
                    if permalink:
                        comment_str += '\nLink: https://www.reddit.com' + permalink
                    else:
                        link_id = comment.get('link_id')
                        if link_id and link_id.startswith('t3_'):
                            comment_str += f'\nLink: https://www.reddit.com/comments/{link_id[3:]}/'
                results.append(comment_str)
            else:
                if FILTER_CONFIG['fields']:
                    filtered_obj = {field: comment.get(field) for field in FILTER_CONFIG['fields']}
                    results.append(json.dumps(filtered_obj, ensure_ascii=False))
                else:
                    results.append(json.dumps(comment, ensure_ascii=False))
        except Exception as e:
            continue
    return results

def batch_lines(iterator, size):
    batch = []
    for item in iterator:
        batch.append(item)
        if len(batch) == size:
            yield batch
            batch = []
    if batch:
        yield batch

def main():
    args = parse_args()
    timestamp = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = args.output or f'filtered_comments_{timestamp}.json'

    with mp.Pool(initializer=init_filter, initargs=(args,)) as pool:
        comment_batches = batch_lines(read_lines_zst(args.file), BATCH_SIZE)
        found_results = False
        comment_count = 0

        with open(filename, 'w', encoding='utf-8') as outf:
            for result_batch in pool.imap(filter_comments, comment_batches):
                if result_batch:
                    found_results = True
                    comment_count += len(result_batch)
                    outf.write('\n'.join(result_batch) + '\n')

    if not found_results:
        print("No results found for the given search parameters.")
    else:
        print(f"Finished. Found and saved {comment_count} comments.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    main()
