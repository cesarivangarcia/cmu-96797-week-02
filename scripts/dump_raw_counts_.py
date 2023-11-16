#!/usr/bin/env python

import duckdb
import sys

raw_tables = [
    "central_park_weather",
    "fhv_bases",
    "fhv_tripdata",
    "fhvhv_tripdata",
    "green_tripdata",
    "yellow_tripdata",
    "bike_data",
]


def main(conn):
    conn.sql(f"create table fhv_bases as select * from  read_csv_auto('./data/fhv_bases.csv', union_by_name=True, filename=True, all_varchar=1, header=True)")
    #for t in sorted(raw_tables):
    rows = conn.sql(f"SELECT COUNT(*) FROM fhv_bases").fetchone()[0]
    print("fhv_bases", rows)


if __name__ == "__main__":
    with duckdb.connect("memory") as conn:
        print(conn)
        main(conn)
