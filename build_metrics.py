"""Build the EDB metric library from the ComCom consolidated ID parquet.
Usage: python build_metrics.py <EDB_ID_Full_*.parquet> <outdir>
Rule: for year-end data, take the value from the most recent disclosure year that reports each
observation year (so restated comparatives override original figures), at network='All'."""
import sys, duckdb, pandas as pd, numpy as np
from metrics_def import METRICS, DERIVED
src, out = sys.argv[1], sys.argv[2]
con = duckdb.connect()
con.execute(f"create table id as select * from read_parquet('{src}') where source='Year end' and network='All'")
rows, audit = [], []
for key,label,group,units,sec,cat,sub,desc in METRICS:
    subc = "sub_category is null" if sub is None else "sub_category = $sub"
    q = f"""select edb, coalesce(obs_yr, disc_yr) yr, disc_yr, value, version from id
            where section like $sec || '%' and category=$cat and {subc} and description=$desc and value is not null"""
    params = {"sec":sec,"cat":cat,"desc":desc} | ({} if sub is None else {"sub":sub})
    d = con.execute(q, params).df()
    if d.empty: audit.append((key,"NO MATCH",0)); continue
    # duplicates within same edb/yr/disc_yr (e.g. multiple rows) -> flag and take first
    dup = d.groupby(["edb","yr","disc_yr"]).size().max()
    # original (as first disclosed) vs latest
    d = d.sort_values(["edb","yr","disc_yr"])
    first = d.groupby(["edb","yr"]).first().reset_index()
    last = d.groupby(["edb","yr"]).last().reset_index()
    m = last.merge(first[["edb","yr","value"]], on=["edb","yr"], suffixes=("","_orig"))
    m["restated"] = ~np.isclose(m.value, m.value_orig, rtol=1e-4, atol=0.5)
    m["metric"] = key
    rows.append(m[["edb","yr","metric","value","value_orig","restated","disc_yr"]])
    audit.append((key,"ok" if dup==1 else f"dup rows x{dup}",len(m)))
long = pd.concat(rows)
long = long[long.yr.between(2013, 2025)]
wide = long.pivot_table(index=["edb","yr"], columns="metric", values="value").reset_index()
for key,label,group,units,expr in DERIVED:
    try: wide[key] = wide.eval(expr)
    except Exception as e: audit.append((key,f"derive failed {e}",0))
wide = wide.replace([np.inf,-np.inf], np.nan)
meta = pd.DataFrame([m[:4] for m in METRICS]+[d[:4] for d in DERIVED], columns=["metric","label","group","units"])
meta["type"] = ["disclosed"]*len(METRICS)+["derived"]*len(DERIVED)
meta["definition"] = [f"{m[4]} | {m[5]} | {m[6] or ''} | {m[7]}" for m in METRICS]+[d[4] for d in DERIVED]
wide.to_csv(f"{out}/edb_metrics_wide.csv", index=False)
long.to_csv(f"{out}/edb_metrics_long_with_restatements.csv", index=False)
meta.to_csv(f"{out}/metric_definitions.csv", index=False)
pd.DataFrame(audit, columns=["metric","status","rows"]).to_csv(f"{out}/build_audit.csv", index=False)
print(pd.DataFrame(audit, columns=["metric","status","rows"]).to_string())
