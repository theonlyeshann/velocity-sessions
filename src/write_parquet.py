"""Convert native-engine staging CSVs to exact challenge Parquet schemas."""
import os
from pathlib import Path
import duckdb

stage = Path(os.environ.get('STAGING_DIR', '/scratch/staging'))
out = Path(os.environ.get('OUTPUT_DIR', '/output'))
out.mkdir(parents=True, exist_ok=True)
con = duckdb.connect(':memory:')
con.execute("SET threads=4")

def esc(path):
    return str(path).replace("'", "''")

schemas = {
    'user_daily': {
        'user_id': 'VARCHAR', 'day': 'VARCHAR', 'sessions': 'BIGINT',
        'events': 'BIGINT', 'revenue_cents': 'BIGINT', 'top_category': 'VARCHAR',
    },
    'campaign_daily': {
        'campaign_id': 'VARCHAR', 'channel': 'VARCHAR', 'day': 'VARCHAR',
        'sessions': 'BIGINT', 'purchasing_sessions': 'BIGINT',
        'revenue_cents': 'BIGINT', 'assisted_sessions': 'BIGINT',
    },
}

for table, schema in schemas.items():
    (out / table).mkdir(parents=True, exist_ok=True)
    src = esc(stage / table / 'result.csv')
    dest = esc(out / table / 'part-00000.parquet')
    cols = ', '.join(f"'{key}': '{kind}'" for key, kind in schema.items())
    expressions = ', '.join(
        (f'CAST("{name}" AS DATE) AS "{name}"' if name == 'day'
         else f'"{name}"') for name in schema
    )
    sql = f"""
    COPY (
        SELECT {expressions}
        FROM read_csv('{src}', header=true, auto_detect=false,
                      columns={{{cols}}}, nullstr='', strict_mode=true)
    ) TO '{dest}' (FORMAT PARQUET, COMPRESSION SNAPPY);
    """
    con.execute(sql)
    n = con.execute(f"SELECT count(*) FROM read_parquet('{dest}')").fetchone()[0]
    print(f'[velocity] {table}: {n} rows written as Parquet', flush=True)
con.close()
