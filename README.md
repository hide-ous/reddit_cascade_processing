# Reddit Cascade and User Subreddit Network Extraction

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

## Purpose
Process Reddit archives to format information cascade data.

## Main files

- `filter_and_build_network.py`	Filters cascades and subreddit activity to generate cleaned cascades and an edge list of user co-occurrence in subreddits.
- `extract_authors.py`	Extracts all unique users from cascades (used to identify users of interest).
- `author_subreddit_counts.py`	Aggregates subreddit contributions for a list of users across comments and submissions folders.
- `generate_cascades.py`	(Implied) Produces cascades from raw comment/submission files. Not shown here but assumed as a dependency.
- `bots.txt`	List of bot usernames to exclude.

## Commands

1. Extract only some fields from a subreddit's archive
    - input: a subreddit's archive, e.g., `science_submissions.zst`, such as those from this URL: https://academictorrents.com/details/1614740ac8c94505e4ecb9d88be8bed7b6afddd4 (all subreddits, 2005 to 2024 included)
    - output: `science_submissions.jsonl` 
      - format `{"author": "shaunc", "id": "mp0o", "link_id": null, "created_utc": 1161180895}`
```bash
python -u reddit_cascade_processing\extract.py -f author id link_id created_utc -o .\data\interim\science_comments.jsonl .\data\external\subreddits24\science_comments.zst
```

2. Extract and format all cascades
   - removes ill-formatted contributions
   - removes `[removed]` and `[deleted]` authors
   - removes bot authors from a list
   - optionally, keeps only the first appearance of an author in a cascade
   - keeps only cascades between a minimum and maximum length, e.g., 5--100
   - input: a pair of files for commments and submissions respectively for a subreddit, e.g., `science_comments.jsonl` and `science_submissions.jsonl` 
   - output: `science_cascades.jsonl` 
      - format `{"t3_123123": [["asd", 1734763627], ["qwe", 1734764088], ...]}`
```bash
python -u .\reddit_cascade_processing\filter_and_format.py -b .\data\external\botnames.txt --dedup_authors --min_len=5 --max_len=100 -o .\data\processed\science_cascades.jsonl .\data\interim\science_comments.jsonl .\data\interim\science_submissions.jsonl
```

4. Extract unique authors
   - Finds all unique author in one or more cascade files
   - input: cascade files 
   - output: `authors.txt` (newline delimited)
```bash
python -u .\reddit_cascade_processing\extract_unique_authors.py -o .\data\interim\authors.txt .\data\processed\conspiracy_cascades.jsonl .\data\processed\science_cascades.jsonl
```
3. 
```bash
python author_subreddit_counts.py \
  --authors authors.txt \
  --data_dir /path/to/data \
  --output subreddit_counts.jsonl
```
Folder structure of /path/to/data:

```bash
/comments/RC_2020-01.zst
/submissions/RS_2020-01.zst
...
```
3. Filter Cascades & Generate Edge List
```bash
python filter_and_build_network.py \
  --cascades cascades.jsonl \
  --subreddit_counts subreddit_counts.jsonl \
  --min_cascade_count 3 \
  --min_cascade_size 5 \
  --max_cascade_size 100 \
  --min_subreddits 3 \
  --year_start 2019 \
  --year_end 2022 \
  --exclude_subreddits politics funny pics \
  --filtered_cascades_out filtered_cascades.jsonl \
  --edge_list_out user_edges.csv
```
This filters cascades and users, excludes certain subreddits, and limits to users in the largest connected component.

# Output
`filtered_cascades.jsonl`: Cleaned cascades with only valid users.
`user_edges.csv`: Weighted edge list of user-user subreddit co-occurrence.
--------

