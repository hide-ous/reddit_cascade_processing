import pandas as pd
import numpy as np
import scipy.stats as st
import argparse
from tqdm import tqdm
from multiprocessing import Pool, cpu_count

def process_node(node, edgelist_df, alpha_threshold):
    """
    Applies the disparity filter to the edges connected to a single node.

    Args:
        node (str): The node being processed.
        edgelist_df (pd.DataFrame): DataFrame with columns 'source', 'target', and 'weight'.
        alpha_threshold (float): Significance level for the filter.

    Returns:
        list: A list of dictionaries representing the backbone edges connected to the node.
    """
    backbone_edges = []
    neighbors_df = edgelist_df[(edgelist_df['source'] == node) | (edgelist_df['target'] == node)].copy()
    if neighbors_df.empty:
        return backbone_edges

    # Calculate degree and total weight for the node
    degree = len(neighbors_df)
    total_weight = neighbors_df['weight'].sum()

    if total_weight == 0 or degree < 2:
        return backbone_edges

    # Calculate alpha for each edge connected to the node
    neighbors_df['alpha'] = neighbors_df.apply(
        lambda row: (row['weight'] / total_weight) ** (degree - 1), axis=1
    )

    # Identify backbone edges based on alpha threshold
    backbone_neighbors = neighbors_df[neighbors_df['alpha'] < alpha_threshold]

    # Add backbone edges, ensuring correct direction
    for index, row in backbone_neighbors.iterrows():
        source = row['source']
        target = row['target']
        weight = row['weight']
        if source == node:
            backbone_edges.append({'source': source, 'target': target, 'weight': weight})
        elif target == node:
            backbone_edges.append({'source': target, 'target': source, 'weight': weight})

    return backbone_edges

def disparity_filter_parallel(edgelist_df, alpha_threshold=0.05, num_processes=None):
    """
    Applies the disparity filter to a weighted edgelist in parallel.

    Args:
        edgelist_df (pd.DataFrame): DataFrame with columns 'source', 'target', and 'weight'.
        alpha_threshold (float, optional): Significance level for the filter. Defaults to 0.05.
        num_processes (int, optional): Number of processes to use for parallelization.
                                       Defaults to the number of CPU cores.

    Returns:
        pd.DataFrame: DataFrame containing the backbone edges.
    """
    unique_nodes = pd.concat([edgelist_df['source'], edgelist_df['target']]).unique()
    if num_processes is None:
        num_processes = cpu_count()

    with Pool(processes=num_processes) as pool:
        results = pool.starmap(process_node, [(node, edgelist_df, alpha_threshold) for node in unique_nodes])

    # Flatten the list of lists and create the backbone DataFrame
    backbone_edges = [edge for sublist in results for edge in sublist]
    backbone_df = pd.DataFrame(backbone_edges).drop_duplicates(subset=['source', 'target']).reset_index(drop=True)

    return backbone_df

def compute_backbone_network(csv_path, alpha=0.05, num_processes=None):
    """
    Reads a weighted edgelist from a CSV file and computes the backbone network using the parallel disparity filter.

    Args:
        csv_path (str): Path to the CSV file containing the weighted edgelist.
                         The CSV should have at least three columns: 'source', 'target', and 'weight'.
        alpha (float, optional): Significance level for the disparity filter. Defaults to 0.05.
        num_processes (int, optional): Number of processes to use for parallelization.
                                       Defaults to the number of CPU cores.

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

    backbone_df = disparity_filter_parallel(edgelist_df.copy(), alpha_threshold=alpha, num_processes=num_processes)

    return backbone_df

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Compute the backbone network from a weighted edgelist using the parallel disparity filter.")
    parser.add_argument("input_file", help="Path to the input CSV file containing the weighted edgelist (source, target, weight columns).")
    parser.add_argument("output_file", help="Path to the output CSV file to save the backbone network.")
    parser.add_argument("--alpha", type=float, default=0.05, help="Significance level (alpha) for the disparity filter (default: 0.05).")
    parser.add_argument("--processes", type=int, default=None, help=f"Number of processes to use for parallelization (default: {cpu_count()}).")

    args = parser.parse_args()

    input_csv_file = args.input_file
    output_csv_file = args.output_file
    alpha_value = args.alpha
    num_processes_value = args.processes

    backbone = compute_backbone_network(input_csv_file, alpha=alpha_value, num_processes=num_processes_value)

    if not backbone.empty:
        print("\nBackbone Network:")
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