import asyncio, io, os, sys, logging, struct, sqlite3, signal, functools, queue, threading, concurrent.futures
import re

import sqlglot

# --- Configuration ---
HOST = os.getenv("PG_HOST", "0.0.0.0")
PORT = int(os.getenv("PG_PORT", "5432"))
# Use persistent file if specified, else shared memory
DB_PATH = os.getenv("PG_DB_PATH", ":memory:")
DEV_MODE = os.getenv("PG_DEV_MODE", "false").lower() == "true"
DB_URI = DB_PATH if DB_PATH != ":memory:" else "file:minipg_mem?mode=memory&cache=shared"

LOG_LEVEL = logging.INFO
logging.basicConfig(level=LOG_LEVEL, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("MiniPG  ")

# --- Some helper consts
_PARAM_RE = re.compile(r'\$(\d+)')

_INT_UNPACK_MAP = {2: '!h', 4: '!i', 8: '!q'}

_PG_BINARY_PACKERS = {
    16: lambda v: b'\x01' if v else b'\x00',  # bool
    23: struct.Struct('!i').pack,             # int4
    20: struct.Struct('!q').pack,             # int8
    21: struct.Struct('!h').pack,             # int2
    700: struct.Struct('!f').pack,
    701: struct.Struct('!d').pack,
}

# -- Async helpers to avoid starting a new thread for every sqlite command

def sqlite_executor(sqlite_connection, sqlite_queue):
        ''' fn() is currently _run_query_sync() '''

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)

        loop = None
        while True:
            fut, fn, args, kwargs = sqlite_queue.get()
            if fut is None:
                break
            loop = loop or fut.get_loop()
            try:
                result = fn(*args, **kwargs)
                loop.call_soon_threadsafe(fut.set_result, result)
            except Exception as e:
                loop.call_soon_threadsafe(fut.set_exception, e)


# --- Protocol Helpers ---

class BufferReader:
    __slots__ = ('data', 'pos')  

    def __init__(self, data):
        self.data = data
        self.pos = 0

    def read_i16(self):
        val = struct.unpack_from('!h', self.data, self.pos)[0]
        self.pos += 2
        return val

    def read_i32(self):
        val = struct.unpack_from('!i', self.data, self.pos)[0]
        self.pos += 4
        return val

    def read_bytes(self, n):
        val = self.data[self.pos : self.pos + n]
        self.pos += n
        return val

    def skip(self, n):
        self.pos += n

    def read_str(self):
        # fast scan for null byte
        null_pos = self.data.find(b'\0', self.pos)
        if null_pos == -1:
            val = self.data[self.pos:]
            self.pos = len(self.data)
            return val.decode('utf-8')
        
        val = self.data[self.pos : null_pos]
        self.pos = null_pos + 1
        return val.decode('utf-8')


class PGProtocol:

    _struct_i = struct.Struct('!i')
    _struct_h = struct.Struct('!h')
    # table_oid(i), col_attr(h), type_oid(i), type_len(h), type_mod(i), format_code(h)
    _struct_row_field = struct.Struct('!ihihih')

    @classmethod
    def pack_msg(cls, msg_type, body):
        return msg_type + cls._struct_i.pack(len(body) + 4) + body

    @classmethod
    def pack_auth_ok(cls):
        return cls.pack_msg(b'R', cls._struct_i.pack(0))

    @classmethod
    def pack_ready_for_query(cls):
        return cls.pack_msg(b'Z', b'I')

    @classmethod
    def pack_parameter_status(cls, key, value):
        body = key.encode('utf-8') + b'\0' + value.encode('utf-8') + b'\0'
        return cls.pack_msg(b'S', body)

    @classmethod
    def pack_param_desc(cls, oids):
        parts = [cls._struct_h.pack(len(oids))]
        parts.extend(cls._struct_i.pack(oid) for oid in oids)
        return cls.pack_msg(b't', b''.join(parts))

    @classmethod
    def pack_row_desc(cls, columns, col_types, formats=None):
        parts = [cls._struct_h.pack(len(columns))]
        pack_field = cls._struct_row_field.pack
        
        for i, name in enumerate(columns):
            parts.append(name.encode('utf-8') + b'\0')
            
            fmt = 0
            if formats:
                if len(formats) == 1: fmt = formats[0]
                elif len(formats) > i: fmt = formats[i]
            
            oid = col_types[i]
            size = 4 if oid == 23 else -1
            parts.append(pack_field(0, 0, oid, size, -1, fmt))
            
        return cls.pack_msg(b'T', b''.join(parts))

    @classmethod
    def pack_data_row(cls, row, formats=None, col_types=None):
        # 'D' message: count(h) + col_len(i) + data
        parts = [cls._struct_h.pack(len(row))]
        pack_int = cls._struct_i.pack
        
        # Pre-calculate formats 
        row_len = len(row)
        if not formats:
            fmt_map = [0] * row_len
        elif len(formats) == 1:
            fmt_map = [formats[0]] * row_len
        else:
            # fallback if formats list is shorter than row
            fmt_map = formats + [0] * (row_len - len(formats))

        for i, col in enumerate(row):
            if col is None:
                parts.append(pack_int(-1))
                continue
            
            # 1 = Binary, 0 = Text
            is_binary = fmt_map[i] == 1 
            oid = col_types[i] if col_types and i < len(col_types) else 25

            val = b''
            if is_binary:
                if isinstance(col, int):
                    if oid == 20:   val = struct.pack('!q', col) # int8
                    elif oid == 21: val = struct.pack('!h', col) # int2
                    else:           val = pack_int(col)          # int4
                elif isinstance(col, bool):
                    val = b'\x01' if col else b'\x00'
                elif isinstance(col, bytes):
                    val = col
                elif isinstance(col, str) and col.isdigit():
                    try: val = pack_int(int(col))
                    except: val = col.encode('utf-8')
                else:
                    val = str(col).encode('utf-8')

            else:  # Text 
                if isinstance(col, bytes):
                    val = col
                elif isinstance(col, bool):
                    val = b't' if col else b'f'
                else:
                    val = str(col).encode('utf-8')
            
            parts.append(pack_int(len(val)))
            parts.append(val)
            
        return cls.pack_msg(b'D', b''.join(parts))


# --- Transpiled Query Cache ---

@functools.lru_cache(maxsize=1024)
def cached_transpile(sql):
    try:
        return sqlglot.transpile(sql, read='postgres', write='sqlite')
    except:
        logger.warn(f'Transpilation failed for SQL: {sql}')
        return [sql]


# --- Server Logic ---

class PGMockServer:

    def __init__(self, db_uri):
        # check_same_thread=False is REQUIRED for asyncio.to_thread usage
        self.conn = sqlite3.connect(db_uri, uri=True, check_same_thread=False, isolation_level=None)
        self.conn.execute("PRAGMA busy_timeout = 5000") 
        self.conn.execute("PRAGMA journal_mode = WAL")
        # self.conn.execute("PRAGMA cache_size = -20000")
        # self.conn.execute("PRAGMA synchronous = NORMAL")
        # self.conn.execute("PRAGMA foreign_keys = ON")

        # Thread for offloaidng time consuming commands to sqlite_executor
        self.sqlite_executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.sqlite_queue = queue.SimpleQueue()
        self.sqlite_thread = threading.Thread(target=sqlite_executor, args=(self.conn, self.sqlite_queue), daemon=True)
        self.sqlite_thread.start()

        #-- Register Mock Postgres Functions for SQLite, used by asyncpg to check/set configuration        
        self.conn.create_function("current_setting", 1, lambda k: 'off')
        # set_config('jit', 'off', false) -> returns the value 'off'
        self.conn.create_function("set_config", 3, lambda key, val, is_local: val)
        self.conn.create_function("version", 0, lambda: "PostgreSQL 17.7 on x86_64-pc-linux-gnu")  
        # dummy PID
        self.conn.create_function("pg_backend_pid", 0, lambda: 12345)

        self.cursor = self.conn.cursor()
        self.prepared_stmts = {} 
        self.portals = {}        

    def close(self):
        try: self.conn.close()
        except: pass

    async def _run_in_sqlite_thread(self, fn, *args, **kwargs):

        loop = asyncio.get_running_loop()

        # future = loop.create_future()
        # self.sqlite_queue.put((future, fn, args, kwargs))

        # Test - Attempt to use ThreadPoolExecutor with a single thread instead of our manual setup
        if kwargs:
            func_wrapper = lambda: fn(*args, **kwargs) # functools.partial(fn, *args, **kwargs)
            res =  loop.run_in_executor(self.sqlite_executor, func)
        res = loop.run_in_executor(self.sqlite_executor, fn, *args)

        return await res


    def _map_sqlite_type_to_pg_oid(self, sqlite_type):
        base_type = sqlite_type.split('(')[0].upper().strip()
        
        # SQLite Type -> Postgres OID
        TYPE_MAP = {
            'INTEGER': 23,   # int4
            'INT': 23,       # int4
            'TEXT': 25,      # text
            'VARCHAR': 25,   # text
            'REAL': 700,     # float4
            'FLOAT': 701,    # float8
            'BLOB': 17,      # bytea
            'BOOLEAN': 16,   # bool
            'BOOL': 16       # bool
        }
        return TYPE_MAP.get(base_type, 25) # Default to Text if unknown


    def _infer_oids_from_schema(self, pg_sql, param_count):
        # Default to Text (25) as a safe fallback
        oids = [25] * param_count
        
        try:
            # 1. Parse SQL to find Table and Columns
            # We use a simple regex for speed/reliability on INSERTs
            # Matches: INSERT INTO table (col1, col2) VALUES ...
            match = re.search(r'INSERT\s+INTO\s+["\']?(\w+)["\']?\s*\(([^)]+)\)', pg_sql, re.IGNORECASE)
            
            if not match:
                return oids

            table_name = match.group(1)
            columns_str = match.group(2)
            # Clean up column names (remove quotes, whitespace)
            target_cols = [c.strip().strip('"\'') for c in columns_str.split(',')]

            # 2. Ask SQLite for the table schema
            # PRAGMA table_info returns: (cid, name, type, notnull, dflt_value, pk)
            schema_rows = self.conn.execute(f"PRAGMA table_info({table_name})").fetchall()
            
            # Create a lookup: {'col_name': 'INTEGER', ...}
            col_type_map = {row[1]: row[2] for row in schema_rows}

            # 3. Map Parameters to Columns
            # We need to find which $N maps to which column. 
            # We assume the VALUES clause matches the column list order.
            
            # Find the VALUES part: VALUES ($1, $2, ...)
            values_match = re.search(r'VALUES\s*\(([^)]+)\)', pg_sql, re.IGNORECASE)
            if values_match:
                values_parts = [v.strip() for v in values_match.group(1).split(',')]
                
                # Zip columns with values
                for i, val in enumerate(values_parts):
                    if i >= len(target_cols): break
                    
                    # Check if this value is a placeholder ($1, $2...)
                    ph_match = re.match(r'\$(\d+)', val)
                    if ph_match:
                        # Postgres uses 1-based indexing for params
                        param_idx = int(ph_match.group(1)) - 1
                        
                        if 0 <= param_idx < param_count:
                            col_name = target_cols[i]
                            sqlite_type = col_type_map.get(col_name, 'TEXT')
                            oids[param_idx] = self._map_sqlite_type_to_pg_oid(sqlite_type)
                            
        except Exception as e:
            logger.error(f"Type Inference Failed: {e}")
            
        return oids

        
    # Blocking SQL operation to be run in a thread
    def _run_query_sync(self, sql_stmts, params):

        rows = []
        executed_count = 0
        last_cmd = "SELECT"
        cols = []  # Initialize to ensure return value exists even if loop skips
        
        for query in sql_stmts:
            if not query.strip(): continue
            if "SERIAL" in query and "CREATE" in query.upper(): 
                query = query.replace(" SERIAL ", " INTEGER ").replace(" BIGSERIAL ", " INTEGER ")
            
            if params and isinstance(params, (list, tuple)) and '@1' in query:
                # Map [val1, val2] to {'1': val1, '2': val2}
                params = {str(i + 1): val for i, val in enumerate(params)}
            self.cursor.execute(query, params)
            
            if self.cursor.description:
                rows = [list(r) for r in self.cursor.fetchall()]
                executed_count = len(rows)
                last_cmd = "SELECT"
                cols = [d[0] for d in self.cursor.description]
            else:
                rows = []
                cols = []
                executed_count = self.cursor.rowcount
                cmd_parts = query.strip().split()
                last_cmd = cmd_parts[0].upper() if cmd_parts else "COMMAND"
        
        return rows, executed_count, last_cmd, cols


    def decode_binary_param(self, oid, data):
        """
        Decodes binary data based on Postgres Type OID.
        See: https://github.com/postgres/postgres/blob/master/src/include/catalog/pg_type.dat
        """

        if data is None: return None
        
        try:
            # Text / Varchar / JSON
            if oid in (25, 1043, 114, 3802): 
                return data.decode('utf-8')
            
            # Boolean
            if oid == 16: return data[0] != 0 # bool (1 byte)

            # Integer Types
            if oid == 23:   return struct.unpack('!i', data)[0] # int4
            elif oid == 21: return struct.unpack('!h', data)[0] # int2
            elif oid == 20: return struct.unpack('!q', data)[0] # int8
            
            # Floating Point
            elif oid == 700: return struct.unpack('!f', data)[0] # float4
            elif oid == 701: return struct.unpack('!d', data)[0] # float8
            
            # Fallback for unknown types (keep as bytes or try utf-8)
            return data
            
        except Exception:
            # If decoding fails, return raw bytes so we don't crash
            return data


    def parse_bind(self, body):
        ''' Used in the 'B' message type in handle_client '''

        buf = BufferReader(body)
        portal_name = buf.read_str()
        stmt_name = buf.read_str()

        stored_oids = self.prepared_stmts.get(stmt_name, {}).get("oids", [])

        num_param_fmt = buf.read_i16()
        param_fmts = [buf.read_i16() for _ in range(num_param_fmt)] if num_param_fmt else [0]

        num_params = buf.read_i16()
        params = []
        for i in range(num_params):
            length = buf.read_i32()
            if length == -1: 
                params.append(None)
            else:
                raw_val = buf.read_bytes(length)
                fmt = param_fmts[i] if i < len(param_fmts) else param_fmts[0]

                if fmt == 0: # Text Mode
                    try: 
                        params.append(raw_val.decode('utf-8'))
                    except: 
                        params.append(raw_val) 

                elif fmt == 1: # Binary Mode
                    oid = stored_oids[i] if i < len(stored_oids) else 0
                    if oid > 0:
                        params.append(self.decode_binary_param(oid, raw_val))
                    else:
                        # Fallback heuristic if no OID found
                        if length in _INT_UNPACK_MAP:
                            # Note: Postgres BigInt is !q (long long), Timestamp is also 8 bytes.
                            params.append(struct.unpack(_INT_UNPACK_MAP[length], raw_val)[0])
                        else:
                            params.append(raw_val)                                

        num_r_fmt = buf.read_i16()
        r_fmts = [buf.read_i16() for _ in range(num_r_fmt)] if num_r_fmt else [0]

        logger.info(f"   [BIND] Portal: '{portal_name}' | Stmt: '{stmt_name}'")
        logger.info(f"   [BIND] Raw Params: {params}")

        return {"portal": portal_name, "stmt": stmt_name, "params": params, "r_formats": r_fmts}


    async def handle_client(self, reader, writer):

        try:
            # 1. Startup / Handshake
            data = await reader.read(8)
            if not data: return
            
            # FIX: Unpack immediately so both branches have access
            length = struct.unpack('!i', data[:4])[0]
            code = struct.unpack('!i', data[4:8])[0]

            if code == 80877103: # SSL Request
                writer.write(b'N') 
                # Read the actual startup message now
                header = await reader.read(4)
                if not header: return
                length = struct.unpack('!i', header)[0]
                await reader.read(length - 4) 
            else:
                # Regular startup - we already read 8 bytes
                await reader.read(length - 8)
            
            writer.write(PGProtocol.pack_auth_ok())
            writer.write(PGProtocol.pack_parameter_status("server_version", "17.7"))
            writer.write(PGProtocol.pack_parameter_status("client_encoding", "UTF8"))
            writer.write(PGProtocol.pack_ready_for_query())
            
            while True:
                m_type = await reader.read(1)
                if not m_type or m_type == b'X': break 
                
                type_char = m_type.decode('ascii', errors='replace')
                if type_char in 'QPBDE': # Query, Parse, Bind, Describe, Execute
                    logger.info(f"-> MSG: {type_char}")

                len_bytes = await reader.read(4)
                if not len_bytes: break
                m_len = struct.unpack('!i', len_bytes)[0]
                body = await reader.read(m_len - 4)
                
                if m_type == b'Q': 
                    sql = body.decode('utf-8').strip('\0')
                    await self.execute_query(sql, writer, send_row_desc=True)
                    writer.write(PGProtocol.pack_ready_for_query())
                
                elif m_type == b'P': 
                    buf = BufferReader(body)
                    stmt_name = buf.read_str()
                    query = buf.read_str()
                    logger.info(f"   [PARSE] Stmt: '{stmt_name}' | SQL: {query}")
                    num_params = buf.read_i16()
                    param_oids = [buf.read_i32() for _ in range(num_params)]
                    self.prepared_stmts[stmt_name] = {"sql": query, "oids": param_oids}
                    writer.write(PGProtocol.pack_msg(b'1', b'')) 
                
                elif m_type == b'B': 
                    bind_data = self.parse_bind(body)
                    prepared = self.prepared_stmts.get(bind_data['stmt'], {})
                    self.portals[bind_data['portal']] = {
                        "sql": prepared.get("sql", "") ,
                        "formats": bind_data['r_formats'],
                        "params": bind_data['params']
                    }
                    writer.write(PGProtocol.pack_msg(b'2', b''))
                
                elif m_type == b'D': 
                    desc_type = body[0:1]
                    name = body[1:].split(b'\0')[0].decode('utf-8')

                    if desc_type == b'S': 
                        prepared = self.prepared_stmts.get(name, {})
                        sql = prepared.get("sql", "")
                        oids = prepared.get("oids", [])
                        await self.execute_query(sql, writer, describe_only=True, send_param_desc=True, 
                            param_oids=oids)
                    else: 
                        # Describes a Portal
                        p = self.portals.get(name, {})
                        sql = p.get("sql", "")
                        formats = p.get("formats", None)
                        params = p.get("params", [])
                        await self.execute_query(sql, writer, describe_only=True, result_formats=formats, 
                            params=params)
                
                elif m_type == b'E': 
                    parts = body.split(b'\0')
                    portal_name = parts[0].decode('utf-8')
                    p = self.portals.get(portal_name, {})
                    sql = p.get("sql", "")
                    formats = p.get("formats", [])
                    params = p.get("params", [])
                    await self.execute_query(sql, writer, send_row_desc=False, result_formats=formats, params=params)

                elif m_type == b'S': 
                    writer.write(PGProtocol.pack_ready_for_query())
                
                await writer.drain()
        except ConnectionResetError:
            pass
        except Exception as e:
            logger.error(f"Transport Error: {e}")
        finally:
            writer.close()
            await writer.wait_closed()


    async def execute_query(self, pg_sql, writer, describe_only=False, send_row_desc=True, send_param_desc=False, result_formats=None, params=[], param_oids=None):
        
        if not pg_sql:
            if describe_only: writer.write(PGProtocol.pack_msg(b'n', b''))
            elif not describe_only: writer.write(PGProtocol.pack_msg(b'I', b''))
            return

        # --- INTERCEPTOR 1: Type Info (Darkest Night) ---
        if "typeinfo_tree" in pg_sql:
            if send_param_desc: 
                writer.write(PGProtocol.pack_param_desc([1028]))
            cols = ["oid", "ns", "name", "kind", "basetype", "elemtype", "elemdelim", "range_subtype", "attrtypoids", "attrnames", "depth", "basetype_name", "elemtype_name", "range_subtype_name"]
            col_types = [23, 25, 25, 25, 23, 23, 25, 23, 23, 25, 23, 25, 25, 25]
            if send_row_desc or describe_only: writer.write(PGProtocol.pack_row_desc(cols, col_types, result_formats))
            if describe_only: return
            rows = [
                [25, "pg_catalog", "text", "b", 0, 0, ",", 0, None, None, 0, None, None, None],
                [23, "pg_catalog", "int4", "b", 0, 0, ",", 0, None, None, 0, None, None, None],
                [16, "pg_catalog", "bool", "b", 0, 0, ",", 0, None, None, 0, None, None, None],
                [0,  "pg_catalog", "unknown", "b", 0, 0, ",", 0, None, None, 0, None, None, None],
            ]
            for r in rows: writer.write(PGProtocol.pack_data_row(r, result_formats))
            writer.write(PGProtocol.pack_msg(b'C', b'SELECT 3\0'))
            return

        # --- INTERCEPTOR 2: JIT / Config Handshake ---
        if "current_setting" in pg_sql and "jit" in pg_sql:
            if send_param_desc: 
                writer.write(PGProtocol.pack_param_desc([]))
            
            # Asyncpg expects 2 columns: 'cur' and 'new', both text (25)
            cols, col_types = ["cur", "new"],  [25, 25] 
            if send_row_desc or describe_only: 
                writer.write(PGProtocol.pack_row_desc(cols, col_types, result_formats))
            
            if describe_only: 
                return

            # Return Data: 'off', 'off'
            writer.write(PGProtocol.pack_data_row(["off", "off"], result_formats))
            writer.write(PGProtocol.pack_msg(b'C', b'SELECT 1\0'))
            return
        
        # --- STANDARD QUERY PROCESSING ---
        # 1. Send Parameter Description (Prepare Phase)
        if send_param_desc:
            if param_oids:
                writer.write(PGProtocol.pack_param_desc(param_oids))
            else:
                # Fast regex for parameter count
                matches = re.findall(r'\$(\d+)', pg_sql)
                count = max(map(int, matches)) if matches else 0
                inferred_oids = self._infer_oids_from_schema(pg_sql, count)
                writer.write(PGProtocol.pack_param_desc(inferred_oids))

        # 2. Transpile (Centralized)
        sql_stmts = await asyncio.to_thread(cached_transpile, pg_sql)

        if params:
            logger.info(f"   [EXEC] Original: {pg_sql}")
            logger.info(f"   [EXEC] SQLite:   {sql_stmts[0]}")
            logger.info(f"   [EXEC] Params:   {[type(p).__name__ + ':' + str(p) for p in params]}")

        # 3. Handle Describe Mode optimizations
        if describe_only:
            upper_sql = pg_sql.upper()

            is_mutation = any(x in upper_sql for x in ("INSERT", "UPDATE", "DELETE"))
            if is_mutation and "RETURNING" not in upper_sql:
                writer.write(PGProtocol.pack_msg(b'n', b''))
                return
            
            # If we need to execute to get columns, but have no params,
            # create dummy NULL params so SQLite doesn't crash.
            if not params and count > 0:
                params = [None] * count
                
            # If placeholders exist but no params, we can't safely execute to find columns
            # so we bail out with 'NoData'
            if any('?' in s for s in sql_stmts) and not params:
                 writer.write(PGProtocol.pack_msg(b'n', b''))
                 return

        try:
            # 4. Execute (Sync logic in thread)
            # rows, executed_count, last_cmd, cols = await asyncio.to_thread(self._run_query_sync, sql_stmts, params)
            rows, executed_count, last_cmd, cols = await self._run_in_sqlite_thread(self._run_query_sync, sql_stmts, params)

            # 5. Send Row Description
            types = []
            if cols:
                types = [
                    20 if 'count' in c.lower() else
                    23 if any(x in c.lower() for x in ['id', 'balance', 'oid']) else 
                    25 
                    for c in cols
                ]
                if send_row_desc or describe_only:
                    writer.write(PGProtocol.pack_row_desc(cols, types, result_formats))
            
            if describe_only: 
                return

            # 6. Message Data
            messages = [PGProtocol.pack_data_row(row, result_formats, types) for row in rows] if rows else []
            
            # 7. Command Complete
            tag = f"{last_cmd} {0 if last_cmd=='INSERT' else ''}{executed_count}"
            if last_cmd in ('BEGIN', 'COMMIT', 'ROLLBACK'): 
                tag = last_cmd
            messages.append(PGProtocol.pack_msg(b'C', tag.encode() + b'\0'))

            writer.write(b''.join(messages))

        except Exception as e:
            logger.error(f"SQL Error: {e} | SQL: {pg_sql}")
            payload = b'SERROR\0C42000\0M' + str(e).encode() + b'\0\0'
            writer.write(PGProtocol.pack_msg(b'E', payload))


# --- Supervisor ---
class SourceFileChanged(Exception): 
    pass


class ServerExit(Exception): 
    pass


async def watch_for_changes():

    script_path = os.path.abspath(__file__)
    try: 
        last_mtime = os.stat(script_path).st_mtime
    except OSError: 
        return
    logger.info(f"Dev Mode: Watching {os.path.basename(script_path)}...")

    while True:
        await asyncio.sleep(1) 
        try: 
            if os.stat(script_path).st_mtime > last_mtime: 
                raise SourceFileChanged()
        except OSError: 
            pass


async def start_server():

    keep_alive = sqlite3.connect(DB_URI, uri=True, check_same_thread=True)

    async def client_connected_cb(reader, writer):
        server = PGMockServer(DB_URI)
        await server.handle_client(reader, writer)

    srv = await asyncio.start_server(client_connected_cb, HOST, PORT, reuse_port=sys.platform != "win32")
    logger.info(f"Serving Mock PG17.7 on {HOST}:{PORT}")

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, lambda: stop_event.set())
    except NotImplementedError:
        # Windows doesn't fully support add_signal_handler in all loops
        pass

    async def wait_for_stop():
        await stop_event.wait()
        print (f'\rAborting server')
        raise ServerExit()

    restart = False
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(srv.serve_forever())
            if DEV_MODE:
                tg.create_task(watch_for_changes())
            tg.create_task(wait_for_stop())
    except* SourceFileChanged:
        restart = True
    except* asyncio.CancelledError:
        pass 
    except* Exception as e:
        if not restart and not stop_event.is_set():
            logger.error(f"Server crashed: {e.exceptions[0]}")

    finally:
        srv.close()
        await srv.wait_closed()
        keep_alive.close()

    return restart


if __name__ == "__main__":

    DEV_MODE = True  # For local development, comment out before committing

    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass

    while True:
        try:
            if not (should_restart := asyncio.run(start_server())):
                break 
            logger.info("Reloading...")
            os.execv(sys.executable, [sys.executable] + sys.argv)
        except KeyboardInterrupt:
            logger.info(f"Ctrl+C pressed ,exiting")
            break