# Reddit Cascade and User Subreddit Network Extraction

<a target="_blank" href="https://cookiecutter-data-science.drivendata.org/">
    <img src="https://img.shields.io/badge/CCDS-Project%20template-328F97?logo=cookiecutter" />
</a>

## Purpose
Process Reddit archives to format information cascade data.

## Commands

1. Extract only some fields from a subreddit's archive
    - input: a subreddit's archive, e.g., `science_submissions.zst`, such as those from this URL: https://academictorrents.com/details/1614740ac8c94505e4ecb9d88be8bed7b6afddd4 (all subreddits, 2005 to 2024 included)
    - output: `science_submissions.jsonl` 
      - format `{"author": "asd", "id": "123", "link_id": null, "created_utc": 1161180895}`
    ```shell
    python -u reddit_cascade_processing\extract.py -f author id link_id created_utc -o .\data\interim\science_comments.jsonl .\data\external\subreddits24\science_comments.zst
    ```

2. Extract and format all cascades
   - removes ill-formatted contributions
   - removes `[removed]` and `[deleted]` authors
   - removes bot authors from a list
   - optionally, keeps only the first appearance of an author in a cascade
   - keeps only cascades between a minimum and maximum length, e.g., 5--100
   - input: a pair of files for comments and submissions respectively for a subreddit, e.g., `science_comments.jsonl` and `science_submissions.jsonl` 
   - output: `science_cascades.jsonl` 
      - format `{"t3_123123": [["asd", 1734763627], ["qwe", 1734764088], ...]}`
    ```shell
    python -u .\reddit_cascade_processing\filter_and_format.py -b .\data\external\botnames.txt --dedup_authors --min_len=5 --max_len=100 -o .\data\processed\science_cascades.jsonl .\data\interim\science_comments.jsonl .\data\interim\science_submissions.jsonl
    ```

3. Extract unique authors
   - Finds all unique author in one or more cascade files
   - input: cascade files 
   - output: `authors.txt` (newline delimited)
    ```shell
    python -u .\reddit_cascade_processing\extract_unique_authors.py -o .\data\interim\authors.txt .\data\processed\conspiracy_cascades.jsonl .\data\processed\science_cascades.jsonl
    ```
4. Count authors' contributions per subreddit per year 
   - input: `authors.txt` and a folder `/path/to/data`
     - `/path/to/data` contains two folders, `comments` and `submission`, each containing Reddit archives:
    ```shell
    /comments/RC_2020-01.zst
    /submissions/RS_2020-01.zst
    ...
    ```
   - e.g., archives found at the following URL (2005 to 2023) https://academictorrents.com/details/56aa49f9653ba545f48df2e33679f014d2829c10
   - output: `author_subreddits.jsonl`
     - format: `{"author": "asd", "data": {"2005": {"reddit.com": 30}, "2006": {"reddit.com": 345, "programming": 91, "science": 2, "freeculture": 1}, }}`
    ```shell
    
    python -u reddit_cascade_processing/count_author_contributions.py \
        data/interim/authors.txt /path/to/data --workers=30 \
        -o=data/interim/author_subreddits.jsonl
    ```
5. Filter cascades and generate edge list
   - removes users who appear in less than `min_cascade_count` (e.g., 5)
   - keeps cascades in 2023 only
   - keeps users who contributed at least `min_subreddits` subreddits (excluding certain subreddits, e.g., 1 excluding science)
   - computes an edge list, where nodes are users and edges are weighted with the number of subreddits they have in common (excluding certain subreddits)
   - extracts and saves the gcc
   - filters the cascades excluding users who are not in the gcc
   - input: 
     - `science_cascades.jsonl`
     - `author_subreddits.jsonl`
   - output: 
   - `science_cascades_filtered.jsonl`: cleaned cascades with only valid users.
   - `science_edgelist.csv` 
     - format:
    ```csv
    source,target,weight
    asd,qwe,37
    ...
    ```
    ```shell
    python -u .\reddit_cascade_processing\filter_cascade_and_build_network.py \ 
    --cascades .\data\processed\science_cascades.jsonl \
    --subreddit_counts .\data\interim\author_subreddits.jsonl \ 
    --min_cascade_count=5 \
    --exclude_subreddits science \
    --filtered_cascades_out .\data\processed\science_cascades_filtered.jsonl \
    --edge_list_out .\data\processed\science_edgelist.csv
    ```
6. Extract the network's backbone
   - applies noise-corrected disparity filtering to the edgelist
   - input: `science_edgelist.csv`
   - output: `science_edgelist_disparity.csv`
   ```shell
   venv/bin/python -u reddit_cascade_processing/disparity_filter.py data/processed/science_edgelist.csv data/processed/science_edgelist_disparity.csv --alpha=0.1 --filter=ncdf --processes=46
   ```

