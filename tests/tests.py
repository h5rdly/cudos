import asyncio, random, time

import asyncpg


DARKEST_NIGHT_QUERY_ASYNCPG = '''
WITH RECURSIVE typeinfo_tree(
        oid, ns, name, kind, basetype, elemtype, elemdelim,
        range_subtype, attrtypoids, attrnames, depth)
    AS (
        SELECT
            ti.oid, ti.ns, ti.name, ti.kind, ti.basetype,
            ti.elemtype, ti.elemdelim, ti.range_subtype,
            ti.attrtypoids, ti.attrnames, 0
        FROM
                (
            SELECT
                t.oid                           AS oid,
                ns.nspname                      AS ns,
                t.typname                       AS name,
                t.typtype                       AS kind,
                (CASE WHEN t.typtype = 'd' THEN
                    (WITH RECURSIVE typebases(oid, depth) AS (
                        SELECT
                            t2.typbasetype      AS oid,
                            0                   AS depth
                        FROM
                            pg_type t2
                        WHERE
                            t2.oid = t.oid
    
                        UNION ALL
    
                        SELECT
                            t2.typbasetype      AS oid,
                            tb.depth + 1        AS depth
                        FROM
                            pg_type t2,
                            typebases tb
                        WHERE
                           tb.oid = t2.oid
                           AND t2.typbasetype != 0
                   ) SELECT oid FROM typebases ORDER BY depth DESC LIMIT 1)
    
                   ELSE NULL
                END)                            AS basetype,
                t.typelem                       AS elemtype,
                elem_t.typdelim                 AS elemdelim,
                COALESCE(
                    range_t.rngsubtype,
                    multirange_t.rngsubtype)    AS range_subtype,
                (CASE WHEN t.typtype = 'c' THEN
                    (SELECT
                        array_agg(ia.atttypid ORDER BY ia.attnum)
                    FROM
                        pg_attribute ia
                        INNER JOIN pg_class c
                            ON (ia.attrelid = c.oid)
                    WHERE
                        ia.attnum > 0 AND NOT ia.attisdropped
                        AND c.reltype = t.oid)
    
                    ELSE NULL
                END)                            AS attrtypoids,
                (CASE WHEN t.typtype = 'c' THEN
                    (SELECT
                        array_agg(ia.attname::text ORDER BY ia.attnum)
                    FROM
                        pg_attribute ia
                        INNER JOIN pg_class c
                            ON (ia.attrelid = c.oid)
                    WHERE
                        ia.attnum > 0 AND NOT ia.attisdropped
                        AND c.reltype = t.oid)
    
                    ELSE NULL
                END)                            AS attrnames
            FROM
                pg_catalog.pg_type AS t
                INNER JOIN pg_catalog.pg_namespace ns ON (
                    ns.oid = t.typnamespace)
                LEFT JOIN pg_type elem_t ON (
                    t.typlen = -1 AND
                    t.typelem != 0 AND
                    t.typelem = elem_t.oid
                )
                LEFT JOIN pg_range range_t ON (
                    t.oid = range_t.rngtypid
                )
                LEFT JOIN pg_range multirange_t ON (
                    t.oid = multirange_t.rngmultitypid
                )
        )
     AS ti
        WHERE
            ti.oid = any($1::oid[])
    
        UNION ALL
    
        SELECT
            ti.oid, ti.ns, ti.name, ti.kind, ti.basetype,
            ti.elemtype, ti.elemdelim, ti.range_subtype,
            ti.attrtypoids, ti.attrnames, tt.depth + 1
        FROM
                (
            SELECT
                t.oid                           AS oid,
                ns.nspname                      AS ns,
                t.typname                       AS name,
                t.typtype                       AS kind,
                (CASE WHEN t.typtype = 'd' THEN
                    (WITH RECURSIVE typebases(oid, depth) AS (
                        SELECT
                            t2.typbasetype      AS oid,
                            0                   AS depth
                        FROM
                            pg_type t2
                        WHERE
                            t2.oid = t.oid
    
                        UNION ALL
    
                        SELECT
                            t2.typbasetype      AS oid,
                            tb.depth + 1        AS depth
                        FROM
                            pg_type t2,
                            typebases tb
                        WHERE
                           tb.oid = t2.oid
                           AND t2.typbasetype != 0
                   ) SELECT oid FROM typebases ORDER BY depth DESC LIMIT 1)
    
                   ELSE NULL
                END)                            AS basetype,
                t.typelem                       AS elemtype,
                elem_t.typdelim                 AS elemdelim,
                COALESCE(
                    range_t.rngsubtype,
                    multirange_t.rngsubtype)    AS range_subtype,
                (CASE WHEN t.typtype = 'c' THEN
                    (SELECT
                        array_agg(ia.atttypid ORDER BY ia.attnum)
                    FROM
                        pg_attribute ia
                        INNER JOIN pg_class c
                            ON (ia.attrelid = c.oid)
                    WHERE
                        ia.attnum > 0 AND NOT ia.attisdropped
                        AND c.reltype = t.oid)
    
                    ELSE NULL
                END)                            AS attrtypoids,
                (CASE WHEN t.typtype = 'c' THEN
                    (SELECT
                        array_agg(ia.attname::text ORDER BY ia.attnum)
                    FROM
                        pg_attribute ia
                        INNER JOIN pg_class c
                            ON (ia.attrelid = c.oid)
                    WHERE
                        ia.attnum > 0 AND NOT ia.attisdropped
                        AND c.reltype = t.oid)
    
                    ELSE NULL
                END)                            AS attrnames
            FROM
                pg_catalog.pg_type AS t
                INNER JOIN pg_catalog.pg_namespace ns ON (
                    ns.oid = t.typnamespace)
                LEFT JOIN pg_type elem_t ON (
                    t.typlen = -1 AND
                    t.typelem != 0 AND
                    t.typelem = elem_t.oid
                )
                LEFT JOIN pg_range range_t ON (
                    t.oid = range_t.rngtypid
                )
                LEFT JOIN pg_range multirange_t ON (
                    t.oid = multirange_t.rngmultitypid
                )
        )
     ti,
            typeinfo_tree tt
        WHERE
            (tt.elemtype IS NOT NULL AND ti.oid = tt.elemtype)
            OR (tt.attrtypoids IS NOT NULL AND ti.oid = any(tt.attrtypoids))
            OR (tt.range_subtype IS NOT NULL AND ti.oid = tt.range_subtype)
            OR (tt.basetype IS NOT NULL AND ti.oid = tt.basetype)
    )
    
    SELECT DISTINCT
        *,
        basetype::regtype::text AS basetype_name,
        elemtype::regtype::text AS elemtype_name,
        range_subtype::regtype::text AS range_subtype_name
    FROM
        typeinfo_tree
    ORDER BY
        depth DESC
'''

async def get_conn():

    conn = await asyncpg.connect(
        host='127.0.0.1', 
        port=5432, 
        user='postgres', 
        database='test', 
        password='pwd',
        ssl=False
    )

    return conn


async def test_basic_flow():

    print("\n--- [Basic Flow] ---")
    conn = await get_conn()
    try:
        await conn.execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT)")
        await conn.execute("DELETE FROM users")
        await conn.execute("INSERT INTO users (name) VALUES ('Bob'), ('Bob2')")
        rows = await conn.fetch("SELECT * FROM users ORDER BY name")
        print(f"Rows: {[dict(r) for r in rows]}")
        assert len(rows) == 2
    finally:
        await conn.close()


async def test_rapid_connect_disconnect():

    print("\n--- [Rapid Connect/Disconnect] ---")
    for _ in range(50):
        try:
            conn = await asyncpg.connect(host='127.0.0.1', port=5432, user='postgres', database='test', password='pwd')
            await conn.close()
        except Exception as e:
            print(f"Failed on attempt {_}: {e}")
            raise
    print("Survived 50 rapid connections")


async def test_returning_clause():

    print("\n--- [RETURNING Clause] ---")
    conn = await get_conn()
    try:
        await conn.execute("CREATE TABLE IF NOT EXISTS tasks (id SERIAL PRIMARY KEY, title TEXT)")
        # This usually breaks simple mocks
        task_id = await conn.fetchval("INSERT INTO tasks (title) VALUES ('Buy Milk') RETURNING id")
        print(f"Returned ID: {task_id}")
        assert isinstance(task_id, int)
    finally:
        await conn.close()


async def test_transactions():

    print("\n--- [Transaction Rollback] ---")
    conn = await get_conn()
    try:
        await conn.execute("CREATE TABLE IF NOT EXISTS finance (balance INT)")
        
        try:
            async with conn.transaction():
                await conn.execute("INSERT INTO finance (balance) VALUES (500)")
                raise Exception("Force Rollback")
        except Exception:
            pass 

        val = await conn.fetchval("SELECT COUNT(*) FROM finance")
        print(f"Rows after rollback: {val}")
        assert val == 0
    finally:
        await conn.close()


async def test_concurrency():

    print("\n--- [Concurrency Stress] ---")
    n_clients = 5
    n_inserts = 20
    
    try:
        conns = [await get_conn() for _ in range(n_clients)]
        await conns[0].execute("CREATE TABLE IF NOT EXISTS users (id SERIAL PRIMARY KEY, name TEXT)")
        await conns[0].execute("DELETE FROM users")

        async def worker(conn, cid):
            for i in range(n_inserts):
                val = f"C{cid}_{i}"
                await conn.execute(f"INSERT INTO users (name) VALUES ('{val}')")
                await asyncio.sleep(random.uniform(0.001, 0.01))

        start = time.time()
        async with asyncio.TaskGroup() as tg:
            [tg.create_task(worker(c, i)) for i, c in enumerate(conns)]
        print(f"Inserted {n_clients * n_inserts} rows in {time.time() - start:.2f}s")
        val = await conns[0].fetchval("SELECT COUNT(*) FROM users")
        print(f"Total rows: {val}")
        assert val == n_clients * n_inserts
    finally:
        for c in conns:
            await c.close()


async def test_error_handling():

    print("\n--- [Error Handling] ---")
    conn = await get_conn()
    try:
        try:
            await conn.execute("SELEC * FROM users")
        except asyncpg.PostgresError as e:
            print(f"Caught expected syntax error: {e}")
        
        val = await conn.fetchval("SELECT 'alive'")
        assert val == 'alive'
    finally:
        await conn.close()


async def test_darkest_night():

    print("\n--- [Darkest Night] ---")
    conn = await get_conn()
    try:
        # The query expects a list of OIDs for $1. We can provide a common one like 'text' (25).
        rows = await conn.fetch(DARKEST_NIGHT_QUERY_ASYNCPG, [25])
        print(f"Rows: {[dict(r) for r in rows]}")
    finally:
        await conn.close()


async def test_parameter_binding():

    print("\n--- [Parameter Binding] ---")
    conn = await get_conn()
    try:
        await conn.execute("CREATE TABLE IF NOT EXISTS inventory (item TEXT, count INT)")
        # asyncpg sends this as Parse -> Bind (with params) -> Execute
        await conn.execute("INSERT INTO inventory (item, count) VALUES ($1, $2)", 'apple', 100)
        
        # Test selection with param
        row = await conn.fetchrow("SELECT count FROM inventory WHERE item = $1", 'apple')
        print(f"Fetched: {row['count']}")
        assert row['count'] == 100
    finally:
        await conn.close()



async def main():

    tests = (
        test_basic_flow,
        test_rapid_connect_disconnect,
        test_returning_clause,
        test_transactions,
        test_concurrency,
        test_error_handling,
        test_darkest_night,
        test_parameter_binding,
    )

    start = time.time()
    for test in tests:
        await test()
    print(f"\nAll tests passed, total time: {time.time() - start:.2f}s")


'''
1. Implement Parameter Injection (Priority #1)

Currently, your B (Bind) message parsing extracts parameter values, but you aren't substituting them into the query.
The Problem: asyncpg sends INSERT INTO x VALUES ($1) and sends ['val'] separately. sqlglot might transpile 
$1 to ?, but you need to pass the list of parameters into self.cursor.execute(sql, parameters).
The Fix: Store parameters in self.portal_params during the Bind phase and retrieve them in the Execute phase to pass to SQLite.

3. Handling pg_catalog Introspection

Tools like DBeaver or ORMs (Django/SQLAlchemy) query pg_catalog tables (pg_type, pg_attribute, pg_constraint) on startup to map types.
Feature: Your CatalogManager is a good start, but you need to actally map the OIDs correctly.

Action: If your app uses UUIDs, you must add the UUID OID (2950) to your pg_type mock, or the client will treat UUIDs as strings (or crash).
'''

if __name__ == "__main__":
    asyncio.run(main())