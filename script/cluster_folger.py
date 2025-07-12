"""

Cluster poems by first line and save to CSV.

"""

import csv
import math
import os
import sqlite3
from functools import lru_cache

import distance
import numpy as np
from sklearn.cluster import AffinityPropagation


@lru_cache(maxsize=6000**2)
def process_first_line(line):
    # print(f"Processing line: {line}")
    line = line.strip().lower()
    # print(f"Stripped and lowercased line: {line}")
    # Remove punctuation and extra spaces
    line = "".join(char for char in line if char.isalnum() or char.isspace())
    # print(f"Removed punctuation: {line}")
    line = line.split()
    # print(f"Processed line: {line}")
    return line


def line_similarity(t1, t2):
    """Calculate the negative Levenshtein distance between two strings."""
    l1 = process_first_line(t1[1])
    l2 = process_first_line(t2[1])
    s1 = {word for word in l1 if len(word) >= 4}
    s2 = {word for word in l2 if len(word) >= 4}

    joined_l1 = " ".join(l1)
    joined_l2 = " ".join(l2)
    # print(f"Max length of lines: {max(len(joined_l1), len(joined_l2))}, common words: {s1.intersection(s2)}")
    d = (
        max(len(joined_l1), len(joined_l2))
        if s1.intersection(s2) == set()
        else distance.levenshtein(joined_l1, joined_l2)
    )
    return -d


def get_or_create_distance_matrix(first_lines, matrix_path="dist_matrix.npy"):
    n = len(first_lines)
    if os.path.exists(matrix_path):
        print(f"Loading distance matrix from {matrix_path}...")
        dist_matrix = np.load(matrix_path)
        if dist_matrix.shape != (n, n):
            raise ValueError("Distance matrix shape does not match number of first lines.")
        return dist_matrix
    dist_matrix = np.zeros((n, n))
    print(f"Calculating distance matrix for {n} first lines...")
    for i in range(n):
        print(f"Processing line {i + 1}/{n}: {first_lines[i][1]}")
        for j in range(i + 1, n):
            dist = line_similarity(first_lines[i], first_lines[j])
            dist_matrix[i][j] = dist
            dist_matrix[j][i] = dist
    np.save(matrix_path, dist_matrix)
    print(f"Distance matrix saved to {matrix_path}.")
    return dist_matrix


def cluster_folger():
    # Connect to the SQLite database
    conn = sqlite3.connect("folger_results.db")
    cursor = conn.cursor()

    # Fetch all first lines from the poems table
    cursor.execute("SELECT id, [First Line] FROM details ORDER BY [First Line];")
    first_lines = [(row[0], row[1]) for row in cursor.fetchall()]

    n = len(first_lines)
    dist_matrix = get_or_create_distance_matrix(first_lines)

    # Perform clustering using Affinity Propagation
    print("Clustering first lines using Affinity Propagation...")
    clustering_model = AffinityPropagation(affinity="precomputed", damping=0.5, max_iter=1000)
    labels = clustering_model.fit_predict(dist_matrix)

    # Save clusters to CSV
    with open("clusters.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Cluster", "Index", "First Line"])
        for cluster_id in np.unique(labels):
            cluster_lines = [first_lines[i] for i in range(n) if labels[i] == cluster_id]
            for witness_id, line in cluster_lines:
                writer.writerow([cluster_id, witness_id, line])

    # Close the database connection
    conn.close()


def create_subclusters():
    conn = sqlite3.connect("folger_results.db")
    cursor = conn.cursor()

    # Fetch all first lines from the poems table, grouped by cluster
    sql = """
    SELECT clusters.cluster, details.id as witness_id, details.[First Line]
    FROM clusters
    JOIN details ON details.id = clusters.[Index]
    ORDER BY clusters.cluster, details.id;"""
    cursor.execute(sql)
    # process each group
    rows = cursor.fetchall()
    print(f"Found {len(rows)} rows.")
    # use the groupby function to group by cluster
    clusters = {}
    for row in rows:
        cluster_id, witness_id, first_line = row
        if cluster_id not in clusters:
            clusters[cluster_id] = []
        clusters[cluster_id].append((witness_id, first_line))
    print(f"Found {len(clusters)} clusters.")
    with open("subclusters.csv", "w", newline="") as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(["Cluster", "Subcluster", "Witness ID", "First Line"])
        for cluster_id, first_lines in clusters.items():
            print(f"Processing cluster {cluster_id} with {len(first_lines)} first lines.")
            # Get the distance matrix for this cluster
            dist_matrix = get_or_create_distance_matrix(first_lines, f"dist_matrix_{cluster_id}.npy")
            # Perform clustering using Affinity Propagation
            clustering_model = AffinityPropagation(affinity="precomputed", damping=0.5, max_iter=1000)
            labels = clustering_model.fit_predict(dist_matrix)
            # save the subclusters to a CSV file
            print(f"Subclustered cluster {cluster_id} into {len(np.unique(labels))} subclusters.")

            for subcluster_id in np.unique(labels):
                subcluster_lines = [first_lines[i] for i in range(len(first_lines)) if labels[i] == subcluster_id]
                for witness_id, line in subcluster_lines:
                    writer.writerow([cluster_id, subcluster_id, witness_id, line])


if __name__ == "__main__":
    pass
    # cluster_folger()
    # create_subclusters()
    # print("Clustering complete. Results saved to clusters.csv.")
