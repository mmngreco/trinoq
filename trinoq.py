"""
You need to define a TRINO_URL environment variable with the connection string to the trino server.

$ export TRINO_URL="https://host:443?user=user@google.com"
$ trinoq "select 1"
"""
import pandas as pd
from rich import print
import os


def create_connection():
    import warnings
    import google.auth
    from google.auth.transport.requests import Request
    from trino.auth import JWTAuthentication
    from trino.dbapi import connect

    # parse url
    from urllib.parse import urlparse, parse_qs

    # parse url
    trino_url = os.environ["TRINO_URL"]
    parsed_url = urlparse(trino_url)

    host = parsed_url.hostname
    port = parsed_url.port
    http_scheme = parsed_url.scheme

    params = parse_qs(parsed_url.query)
    user = params["user"][0]
    catalog = params.get("catalog", [None])[0]
    schema = params.get("schema", [None])[0]

    # auth
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        credentials, _ = google.auth.default()
        credentials.refresh(Request())
        auth = JWTAuthentication(credentials.token)

    # connect
    conn = connect(
        auth=auth,
        user=user,
        host=host,
        port=port,
        http_scheme=http_scheme,
        catalog=catalog,
        schema=schema,
    )
    return conn


def get_query(args):
    query_in = args.query
    try:
        with open(query_in, "r") as f:
            out = f.read()
    except FileNotFoundError:
        out = query_in
    return out


def get_args():
    import argparse

    parser = argparse.ArgumentParser(description="Query")
    parser.add_argument("query", help="query or filename with query")
    parser.add_argument(
        "-e",
        "--eval-df",
        help="Evaluate 'df' using string or filename",
        default="",
    )
    return parser.parse_args()


def get_eval_df(args):
    eval_df_in = args.eval_df
    try:
        with open(eval_df_in, "r") as f:
            out = f.read()
    except FileNotFoundError:
        out = eval_df_in
    return out


def get_temp_file(query):
    from hashlib import sha1
    from pathlib import Path

    qhash = sha1(query.encode()).hexdigest()
    temp_file = Path(f"/tmp/druidq/{qhash}.parquet")
    if not temp_file.parent.exists():
        temp_file.parent.mkdir(parents=True, exist_ok=True)

    return temp_file


def execute(query, conn):
    import warnings

    # cache {{
    temp_file = get_temp_file(query)
    if temp_file.exists():
        print(f"Loading cache: {temp_file}")
        return pd.read_parquet(temp_file)
    # }}

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = pd.read_sql(query, conn)

    # cache {{
    print(f"Saving cache: {temp_file}")
    df.to_parquet(temp_file)
    # }}

    return df


def app():
    args = get_args()
    query = get_query(args)
    print("In[query]:")
    print(query)

    conn = create_connection()
    df = execute(query, conn)

    print()
    print("Out[df]:")
    print(df)

    if args.eval_df:
        eval_df = get_eval_df(args)
        print()
        print("In[eval]:")
        print(eval_df)

        print("Out[eval]:")
        exec(eval_df, globals(), locals())


if __name__ == "__main__":
    app()
