import pandas as pd
import numpy as np
import scipy.stats as st
import argparse
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
import math
import time

def calculate_ncdf_alpha_numpy(weight, total_weight, degree):
    if degree < 2 or total_weight == 0:
        return 1.0
    p = weight / total_weight
    return 1 - (1 - p) ** (degree - 1)

def process_node_batch_numpy(node_batch, edgelist_df, alpha_threshold, filter_type):
    backbone_edges = set()
    for node in node_batch:
        neighbors_df = edgelist_df[(edgelist_df['source'] == node) | (edgelist_df['target'] == node)]
        if neighbors_df.empty:
            continue

        weights = neighbors_df['weight'].to_numpy()
        degree = len(weights)
        total_weight = np.sum(weights)

        if total_weight == 0 or degree < 2:
            continue

        if filter_type == 'disparity':
            probabilities = weights / total_weight
            alpha_values = probabilities ** (degree - 1)
            backbone_indices = np.where(alpha_values < alpha_threshold)[0]
        elif filter_type == 'ncdf':
            probabilities = weights / total_weight
            alpha_values = calculate_ncdf_alpha_numpy(weights, total_weight, degree)
            backbone_indices = np.where(alpha_values < alpha_threshold)[0]
        else:
            raise ValueError(f"Unknown filter type: {filter_type}")

        for index in backbone_indices:
            row = neighbors_df.iloc[index]
            u = row['source']
            v = row['target']
            weight = row['weight']
            sorted_edge = tuple(sorted((u, v)))
            backbone_edges.add((sorted_edge[0], sorted_edge[1], weight))

    return list(backbone_edges)

def disparity_filter_parallel_batched_numpy(edgelist_df, alpha_threshold=0.05, num_processes=None, batch_size=100, filter_type='disparity'):
    unique_nodes = pd.concat([edgelist_df['source'], edgelist_df['target']]).unique()
    num_nodes = len(unique_nodes)
    if num_processes is None:
        num_processes = cpu_count()

    node_batches = [unique_nodes[i:i + batch_size] for i in range(0, num_nodes, batch_size)]

    backbone_edges = set()
    with Pool(processes=num_processes) as pool:
        results = list(tqdm(pool.starmap(process_node_batch_numpy, [(batch, edgelist_df, alpha_threshold, filter_type) for batch in node_batches]), total=len(node_batches), desc="Processing batches"))
        for sublist in results:
            for edge in sublist:
                backbone_edges.add(edge)

    backbone_list = [{'source': u, 'target': v, 'weight': weight} for u, v, weight in backbone_edges]
    backbone_df = pd.DataFrame(backbone_list)

    return backbone_df

def compute_backbone_network_numpy(csv_path, alpha=0.05, num_processes=None, filter_type='disparity', batch_size=100):
    start_time = time.time()
    try:
        edgelist_df = pd.read_csv(csv_path)
        if not all(col in edgelist_df.columns for col in ['source', 'target', 'weight']):
            print("Error: CSV file must contain 'source', 'target', and 'weight' columns.")
            return pd.DataFrame()
        if edgelist_df.empty:
            print("Warning: Input CSV file is empty.")
            return pd.DataFrame()
    except FileNotFoundError:
        print(f"Error: File not found at {csv_path}")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred while reading the CSV file: {e}")
        return pd.DataFrame()

    backbone_df = disparity_filter_parallel_batched_numpy(
        edgelist_df.copy(),
        alpha_threshold=alpha,
        num_processes=num_processes,
        batch_size=batch_size,
        filter_type=filter_type
    )

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"\nBackbone computation finished in {elapsed_time:.2f} seconds.")

    return backbone_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute backbone network with batched parallel processing (NumPy optimized) with timer and progress bar.")
    parser.add_argument("input_file", help="Path to input CSV.")
    parser.add_argument("output_file", help="Path to output CSV.")
    parser.add_argument("--alpha", type=float, default=0.05, help="Alpha threshold.")
    parser.add_argument("--processes", type=int, default=None, help="Number of processes.")
    parser.add_argument("--filter", type=str, default='disparity', choices=['disparity', 'ncdf'], help="Filter type.")
    parser.add_argument("--batch_size", type=int, default=100, help="Number of nodes per batch.")

    args = parser.parse_args()

    backbone = compute_backbone_network_numpy(
        args.input_file,
        alpha=args.alpha,
        num_processes=args.processes,
        filter_type=args.filter,
        batch_size=args.batch_size
    )

    if not backbone.empty:
        print(f"\nBackbone Network (using {args.filter} filter):")
        print(backbone)
        num_edges = len(backbone)
        num_nodes = pd.concat([backbone['source'], backbone['target']]).nunique()
        print(f"\nNumber of edges: {num_edges}")
        print(f"Number of nodes: {num_nodes}")
        try:
            backbone.to_csv(args.output_file, index=False)
            print(f"\nBackbone saved to {args.output_file}")
        except Exception as e:
            print(f"Error saving to CSV: {e}")
    else:
        print("\nNo backbone network computed.")