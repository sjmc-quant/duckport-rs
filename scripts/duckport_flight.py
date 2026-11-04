"""Minimal duckport Flight client shared by the scripts in this directory.

Only needs pyarrow, so it runs from any ingestor venv. Loopback servers need no token;
otherwise the Bearer token comes from DUCKPORT_TOKEN.
"""

import base64
import json
import math
import os

import pyarrow as pa
import pyarrow.flight as flight


class _Bearer(flight.ClientMiddleware):
    def __init__(self, token):
        self._value = f"Bearer {token}".encode()

    def sending_headers(self):
        return {b"authorization": self._value}


class _BearerFactory(flight.ClientMiddlewareFactory):
    def __init__(self, token):
        self._token = token

    def start_call(self, info):
        return _Bearer(self._token)


def flight_client_kwargs(token):
    """Bearer middleware when a token is set. An empty token stays anonymous."""
    if not token:
        return {}
    return {"middleware": [_BearerFactory(token)]}


class Duckport:
    def __init__(self, addr, token=None):
        """Connect to a duckport Flight endpoint.

        ``token=None`` keeps the old behaviour and reads ``DUCKPORT_TOKEN``.
        Pass ``token=""`` to stay anonymous even when that variable is set.
        """
        if token is None:
            token = os.getenv("DUCKPORT_TOKEN", "")
        self.client = flight.FlightClient(f"grpc://{addr}", **flight_client_kwargs(token))

    def action(self, kind, body=None):
        payload = json.dumps(body).encode() if body is not None else b""
        res = list(self.client.do_action(flight.Action(kind, payload)))
        return json.loads(res[0].body.to_pybytes())

    def execute(self, sql):
        return self.action("duckport.execute", {"sql": sql})["rows_affected"]

    def transaction(self, statements):
        return self.action("duckport.execute_transaction", {"statements": statements})

    def query(self, sql):
        """Rows as dicts. duckport.query raises on an empty result; that becomes []."""
        try:
            table = self.query_arrow(sql)
        except flight.FlightInternalError as e:
            if "never sent a data message" in str(e):
                return []
            raise
        if table.num_columns == 0 or table.num_rows == 0:
            return []
        return table.to_pylist()

    def query_arrow(self, sql):
        """Arrow result of a read-only query. An empty result is a zero-column table."""
        ticket = flight.Ticket(json.dumps({"type": "duckport.query", "sql": sql}).encode())
        try:
            return self.client.do_get(ticket).read_all()
        except flight.FlightInternalError as e:
            if "never sent a data message" in str(e):
                return pa.table({})
            raise

    def append(self, schema, table, data):
        """DoPut ``data`` into ``schema.table``. The table must already exist."""
        desc = flight.FlightDescriptor.for_path("duckport.append", schema, table)
        writer, reader = self.client.do_put(desc, data.schema)
        rows = data.num_rows or 1
        per_row = max(1, math.ceil(data.nbytes / rows)) if data.num_rows else 1
        writer.write_table(data, max_chunksize=max(1, (32 * 1024 * 1024) // per_row))
        writer.done_writing()
        buf = reader.read()
        writer.close()
        if not buf:
            return {}
        return json.loads(bytes(buf))

    def scalar(self, sql):
        rows = self.query(sql)
        return list(rows[0].values())[0] if rows else None


def q(name):
    """Quote an identifier."""
    return '"' + name.replace('"', '""') + '"'


def lit(value):
    """Quote a string literal."""
    return "'" + str(value).replace("'", "''") + "'"


def text(value):
    """String value for an expression. Non-ASCII text goes base64-encoded so the SQL stays
    ASCII: duckport-server <= 1.1.0 panics when its log line cuts the SQL inside a
    multi-byte character, after the statement has already run."""
    value = str(value)
    if value.isascii():
        return lit(value)
    return f"decode(from_base64({lit(base64.b64encode(value.encode()).decode())}))"
