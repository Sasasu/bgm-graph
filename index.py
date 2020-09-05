#!/bin/python3

import os
import json
import time
import typing
import argparse

from nebula.Client import GraphClient, AuthException, ExecutionException
from nebula.ConnectionPool import ConnectionPool

SCHEMA = ""
SCHEMA += "CREATE SPACE IF NOT EXISTS bgm;"
SCHEMA += "USE bgm;"
SCHEMA += "CREATE TAG IF NOT EXISTS node("
SCHEMA += "name                string,"
# TODO 转义
# SCHEMA += "summary             string,"
SCHEMA += 'labels              string DEFAULT "",'  # 以 | 分割
SCHEMA += "type                int,"
SCHEMA += "rating_score        double DEFAULT 0.0"
SCHEMA += ");"

SCHEMA += "CREATE EDGE IF NOT EXISTS related(type string);"


def insert_vertex(ID: int, values: typing.Tuple[str, typing.Any]) -> str:
    ret = f"INSERT VERTEX NO OVERWRITE node({ ','.join([i[0] for i in values]) }) VALUES {ID}:("
    for v in values:
        v = v[1]
        if type(v) == str:
            ret += '"'
            ret += v.replace("\n", "")
            ret += '",'
        if type(v) == int:
            ret += str(v)
            ret += ","
        if type(v) == float:
            ret += format(v, '.20f')
            ret += ","
    ret = ret[:len(ret)-1]
    ret += ");"
    return ret


def insert_edge(fr: int, to: int, values: typing.Tuple[str, typing.Any]) -> str:
    ret = f"INSERT EDGE NO OVERWRITE related({ ','.join([i[0] for i in values]) }) VALUES {fr} -> {to}:("
    for v in values:
        v = v[1]
        if type(v) == str:
            ret += '"'
            ret += v.replace("\n", "")
            ret += '",'
        if type(v) == int:
            ret += str(v)
            ret += ","
        if type(v) == float:
            ret += format(v, '.20f')
            ret += ","
    ret = ret[:len(ret)-1]
    ret += ");"
    return ret


class Arg:
    addr: str
    port: int
    p: str
    u: str
    data: str


def arg_parse() -> Arg:
    parser = argparse.ArgumentParser(description='Create Nebula Graph schema')
    parser.add_argument('--addr', default='127.0.0.1', metavar='',
                        type=str, help='Nebula daemon IP address')
    parser.add_argument('--port', default=3699, type=int, metavar='',
                        help='an integer for the accumulator')
    parser.add_argument('-p', default='root', type=str, metavar='',
                        help='Password used to authenticate')
    parser.add_argument('-u', default='nebula', type=str, metavar='',
                        help='Username used to authenticate')
    parser.add_argument('-data', default="./Bangumi-Subject/data/",
                        type=str, metavar='', help='The data folder path')

    return parser.parse_args()


def create_client(arg: Arg) -> typing.Optional[GraphClient]:
    connection_pool = ConnectionPool(arg.addr, arg.port)
    client = GraphClient(connection_pool)
    client.set_space('bgm')
    try:
        client.authenticate(arg.u, arg.p)
    except AuthException as e:
        print(e)
        return None

    return client


def create_schema(client: GraphClient):
    resp = client.execute(SCHEMA)
    if resp.error_code != 0:
        print(f"schema create failure {resp.error_msg}")


def get_data_file(name: str) -> typing.Iterable[str]:
    for root, dirs, files in os.walk(name, topdown=False):
        for name in files:
            yield os.path.join(root, name)


def read_data_to_vertex(file: str) -> typing.Iterable[str]:
    with open(file, "rb") as F:
        data = json.loads(F.read())
        ID = data["id"]
        values = [
            ("name", data["name"]),
            # ("summary", data.get("summary", '')[0:16]),
            ("labels", "|".join([i["name"] for i in data.get("tags", [])])),
            ("type", data["type"]),
            ("rating_score", float(data.get("rating", {"score": 0})["score"]))
        ]
        return insert_vertex(ID, values)


def read_data_to_edge(file: str) -> typing.Iterable[str]:
    with open(file, "rb") as F:
        data = json.loads(F.read())
        fr = data["id"]
        for relation in data.get("relations", []):
            to = relation["id"]
            t = relation["type"]
            yield insert_edge(fr, to, [("type", t)])


def main():
    arg = arg_parse()
    client = create_client(arg)
    if not client:
        return

    create_schema(client)
    time.sleep(15)

    for i in get_data_file(arg.data):
        vertex = read_data_to_vertex(i)
        V = client.execute(vertex)
        if V.error_code != 0:
            print(f"{vertex} {V.error_msg}")

    for i in get_data_file(arg.data):
        for edge in read_data_to_edge(i):
            V = client.execute(edge)
            if V.error_code != 0:
                print(f"{edge} {V.error_msg}")

if __name__ == '__main__':
    main()
