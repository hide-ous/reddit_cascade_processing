import pandas as pd
import numpy as np
import scipy.stats as st
import argparse
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

def calculate_ncdf_alpha(weight, total_weight, degree):
    """Calculates the alpha value for the Noise Corrected Disparity Filter."""
    if degree < 2 or total_weight == 0:
        return 1.0
    p = weight / total_weight
    return 1 - (1 - p) ** (degree - 1)

def process_node_ncdf(node, edgelist_df, alpha_threshold):
    """
    Applies the Noise Corrected Disparity Filter to the edges connected to a single node.

    Args:
        node (str): The node being processed.
        edgelist_df (pd.DataFrame): DataFrame with columns 'source', 'target', and 'weight'.
        alpha_threshold (float): Significance level for the filter.

    Returns:
        list: A list of tuples representing the backbone edges connected to the node
              (sorted node pair to avoid duplicates).
    """
    backbone_edges = set()
    neighbors_df = edgelist_df[(edgelist_df['source'] == node) | (edgelist_df['target'] == node)].copy()
    if neighbors_df.empty:
        return list(backbone_edges)

    # Calculate degree and total weight for the node
    degree = len(neighbors_df)
    total_weight = neighbors_df['weight'].sum()

    if total_weight == 0 or degree < 2:
        return list(backbone_edges)

    # Calculate NCDF alpha for each edge
    neighbors_df['alpha_ncdf'] = neighbors_df.apply(
        lambda row: calculate_ncdf_alpha(row['weight'], total_weight, degree), axis=1
    )

    # Identify backbone edges based on alpha threshold
    backbone_neighbors = neighbors_df[neighbors_df['alpha_ncdf'] < alpha_threshold]

    # Add backbone edges (as sorted tuples)
    for index, row in backbone_neighbors.iterrows():
        u = row['source']
        v = row['target']
        weight = row['weight']
        sorted_edge = tuple(sorted((u, v)))
        backbone_edges.add((sorted_edge[0], sorted_edge[1], weight))

    return list(backbone_edges)

def disparity_filter_ncdf_parallel(edgelist_df, alpha_threshold=0.05, num_processes=None):
    """
    Applies the Noise Corrected Disparity Filter to a weighted edgelist in parallel.

    Args:
        edgelist_df (pd.DataFrame): DataFrame with columns 'source', 'target', and 'weight'.
        alpha_threshold (float, optional): Significance level for the filter. Defaults to 0.05.
        num_processes (int, optional): Number of processes to use for parallelization.
                                       Defaults to the number of CPU cores.

    Returns:
        pd.DataFrame: DataFrame containing the unique backbone edges.
    """
    unique_nodes = pd.concat([edgelist_df['source'], edgelist_df['target']]).unique()
    if num_processes is None:
        num_processes = cpu_count()

    with Pool(processes=num_processes) as pool:
        results = pool.starmap(process_node_ncdf, [(node, edgelist_df, alpha_threshold) for node in unique_nodes])

    # Flatten the list of sets and create the backbone DataFrame
    backbone_edges = set()
    for sublist in results:
        for edge in sublist:
            backbone_edges.add(edge)

    backbone_list = [{'source': u, 'target': v, 'weight': weight} for u, v, weight in backbone_edges]
    backbone_df = pd.DataFrame(backbone_list)

    return backbone_df

def compute_backbone_network(csv_path, alpha=0.05, num_processes=None, filter_type='disparity'):
    """
    Reads a weighted edgelist from a CSV file and computes the backbone network using the specified filter.

    Args:
        csv_path (str): Path to the CSV file containing the weighted edgelist (source, target, weight columns).
        alpha (float, optional): Significance level for the disparity filter. Defaults to 0.05.
        num_processes (int, optional): Number of processes to use for parallelization.
                                       Defaults to the number of CPU cores.
        filter_type (str, optional): The type of filtering to apply ('disparity' or 'ncdf').
                                     Defaults to 'disparity'.

    Returns:
        pandas.DataFrame: DataFrame representing the backbone network with 'source', 'target', and 'weight' columns.
                          Returns an empty DataFrame if the input CSV is invalid or empty.
    """
    try:
        edgelist_df = pd.read_csv(csv_path)
        if not all(col in edgelist_df.columns for col in ['source', 'target', 'weight']):
            print("Error: CSV file must contain columns 'source', 'target', and 'weight'.")
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

    if filter_type == 'disparity':
        backbone_df = disparity_filter_parallel(edgelist_df.copy(), alpha_threshold=alpha, num_processes=num_processes)
    elif filter_type == 'ncdf':
        backbone_df = disparity_filter_ncdf_parallel(edgelist_df.copy(), alpha_threshold=alpha, num_processes=num_processes)
    else:
        print(f"Error: Unknown filter type '{filter_type}'. Choose 'disparity' or 'ncdf'.")
        return pd.DataFrame()

    return backbone_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute the backbone network from a symmetric and undirected weighted edgelist using the parallel disparity or noise corrected disparity filter.")
    parser.add_argument("input_file", help="Path to the input CSV file containing the weighted edgelist (source, target, weight columns).")
    parser.add_argument("output_file", help="Path to the output CSV file to save the backbone network.")
    parser.add_argument("--alpha", type=float, default=0.05, help="Significance level (alpha) for the filter (default: 0.05).")
    parser.add_argument("--processes", type=int, default=None, help=f"Number of processes to use for parallelization (default: {cpu_count()}).")
    parser.add_argument("--filter", type=str, default='disparity', choices=['disparity', 'ncdf'], help="Type of filtering to apply ('disparity' or 'ncdf') (default: disparity).")

    args = parser.parse_args()

    input_csv_file = args.input_file
    output_csv_file = args.output_file
    alpha_value = args.alpha
    num_processes_value = args.processes
    filter_type_value = args.filter

    backbone = compute_backbone_network(input_csv_file, alpha=alpha_value, num_processes=num_processes_value, filter_type=filter_type_value)

    if not backbone.empty:
        print(f"\nBackbone Network (using {filter_type_value} filter):")
        print(backbone)

        num_backbone_edges = len(backbone)
        backbone_nodes = pd.concat([backbone['source'], backbone['target']]).nunique()
        print(f"\nNumber of edges in the backbone network: {num_backbone_edges}")
        print(f"Number of nodes in the backbone network: {backbone_nodes}")

        try:
            backbone.to_csv(output_csv_file, index=False)
            print(f"\nBackbone network saved to: {output_csv_file}")
        except Exception as e:
            print(f"Error saving the backbone network to CSV: {e}")
    else:
        print("\nNo backbone network could be computed.")