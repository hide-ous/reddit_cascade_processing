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
README.md	Project documentation.
## Commands 
1. Extract Unique Authors from Cascades
```bash
python extract_authors.py --cascades cascades.jsonl --output authors.txt
```

2. Generate Subreddit Activity Counts
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

