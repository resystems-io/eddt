#!/bin/python

#
# A quick check of the avro schemas using fastavro.
# Used manually to confirm the behaviour of hamba/avro and duckdb avro.
#

import argparse
import fastavro
import json
import pandas as pd
import sys

## Check schema parsing which would be needed for writes.

def check_avro_schema():
    # Create a shared symbol table
    symbol_table = {}

    # Load the raw JSON
    with open("avro_delta.avsc") as f:
        delta_types_schema = json.load(f)
    with open("avro_geo.avsc") as f:
        geo_types_schema = json.load(f)
    with open("avro_scooter.avsc") as f:
        scooter_schema = json.load(f)

    # Parse the dependencies first to populate the symbol table
    fastavro.parse_schema(delta_types_schema, named_schemas=symbol_table)
    fastavro.parse_schema(geo_types_schema, named_schemas=symbol_table)

    # Dump symbols for debugging
    print("Known Types in Symbol Table:")
    for known_type in symbol_table.keys():
        print(f" - {known_type}")

    # Parse the main schema
    parsed_scooter_schema = fastavro.parse_schema(scooter_schema, named_schemas=symbol_table)


## For reading, just laod
def check_reading():

    # find the max speed
    max_speed = 0.0
    max_motor = 0.0

    with open("/tmp/updates.avro", "rb") as in_scooters:
        # initialise the reader
        reader = fastavro.reader(in_scooters) # , return_record_name=True)

        # iterate over records
        for record in reader:
            # print(f"Scooter Event: {record}")
            # find the maximum road speed
            speed_union = record.get('speedometer_speed')
            if False:
                print(f"speed_union: {speed_union}")
                print(f"speed_union type: {type(speed_union)}")
            if isinstance(speed_union, float):
                speed = speed_union
                if speed > max_speed:
                    max_speed = speed
            # find the maximum temperature
            motor_union = record.get('motor_temperature')
            if False:
                print(f"motor_union: {motor_union}")
                print(f"motor_union type: {type(motor_union)}")
            if isinstance(motor_union, float):
                motor = motor_union
                if motor > max_motor:
                    max_motor = motor

    # show the max speed
    print(f"Max Speed: {max_speed}")
    print(f"Max Motor: {max_motor}")

def convert_avro_to_parquet_using_fastavro(input_path: str, output_path: str):

    with open(input_path, "rb") as in_scooters:
        # initialise the reader
        reader = fastavro.reader(in_scooters) # , return_record_name=True)
        records = list(reader)

    print(f"Loaded {len(records)} records")

    df = pd.DataFrame(records)
    df.to_parquet(output_path)

    print("Converted to parquet using fastavro", file=sys.stderr)

def convert_avro_to_parquet_using_polars(avro_path, parquet_path):
    import polars as pl
    # read the avro into a Polars Dataframe
    df = pl.read_avro(avro_path)
    print(f"Loaded {df.shape[0]} records from {avro_path}", file=sys.stderr)
    # write the dataframe to parquet
    df.write_parquet(parquet_path)

    print("Converted to parquet using polars", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(description="Tools for working with Deltaflow.")
    parser.add_argument("--avro-deltaflow-path", help="Path to the avro delta file.")
    parser.add_argument("--parquet-deltaflow-path", help="Path to the parquet delta file.")
    parser.add_argument("--mode", choices=[
        "check-schema",
        "convert-to-parquet",
        "convert-to-parquet-polars"
        ], default="convert-to-parquet", help="Mode of operation.")

    args = parser.parse_args()

    if args.mode == "check-schema":
        check_avro_schema()
    elif args.mode == "convert-to-parquet":
        convert_avro_to_parquet_using_fastavro(args.avro_deltaflow_path, args.parquet_deltaflow_path)
    elif args.mode == "convert-to-parquet-polars":
        convert_avro_to_parquet_using_polars(args.avro_deltaflow_path, args.parquet_deltaflow_path)
    else:
        print(f"Unknown mode: {args.mode}", file=sys.stderr)
        parser.print_help()
        sys.exit(1)

# main
if __name__ == "__main__":
    main()
