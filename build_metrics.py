"""Build the EDB metric library from the ComCom consolidated ID parquet.
Usage: python build_metrics.py <EDB_ID_Full_*.parquet> <outdir>
Rule: for year-end data, take the value from the most recent disclosure year that reports each
observation year (so restated comparatives override original figures), at network='All'."""
import sys, duckdb, pandas as pd, numpy as np
from metrics_def import METRICS, DERIVED, COMBINED, COMBINE_RULE
import os
src, out = sys.argv[1], sys.argv[2]
# Line change register: earlier names for the same line (rename = stitch silently; definition_change = stitch and flag)
_reg = os.path.join(os.path.dirname(os.path.abspath(__file__)), "line_changes.csv")
REG = pd.read_csv(_reg) if os.path.exists(_reg) else pd.DataFrame(columns=["stitch"])
REG = REG[REG.stitch.isin(["yes","flag"])]
def _same(a, b): return (pd.isna(a) and b is None) or a == b
con = duckdb.connect()
con.execute(f"create table id as select * from read_parquet('{src}') where source='Year end' and network='All'")
rows, audit = [], []
for key,label,group,units,sec,cat,sub,desc in METRICS:
    subc = "sub_category is null" if sub is None else "sub_category = $sub"
    q = f"""select edb, coalesce(obs_yr, disc_yr) yr, disc_yr, value, version from id
            where section like $sec || '%' and category=$cat and {subc} and description=$desc and value is not null"""
    params = {"sec":sec,"cat":cat,"desc":desc} | ({} if sub is None else {"sub":sub})
    d = con.execute(q, params).df(); d["basis"] = "current"
    # earlier line names from the register
    for _, r in REG.iterrows():
        if not sec.startswith(str(r.section)) and not str(r.section).startswith(sec): continue
        if not (_same(r.new_sub_category, sub) and r.new_description == desc and r.new_category in (cat, "*")): continue
        ocat = cat if r.old_category == "*" else r.old_category
        osub = None if pd.isna(r.old_sub_category) else r.old_sub_category
        osc = "sub_category is null" if osub is None else "sub_category = $sub"
        oq = f"""select edb, coalesce(obs_yr, disc_yr) yr, disc_yr, value, version from id
            where section like $sec || '%' and category=$cat and {osc} and description=$desc and value is not null"""
        od = con.execute(oq, {"sec":sec,"cat":ocat,"desc":r.old_description} | ({} if osub is None else {"sub":osub})).df()
        od["basis"] = f"{r.id} {r.change_type}"
        d = pd.concat([d, od[~od.set_index(["edb","yr","disc_yr"]).index.isin(d.set_index(["edb","yr","disc_yr"]).index)]])
    if d.empty: audit.append((key,"NO MATCH",0)); continue
    # duplicates within same edb/yr/disc_yr (e.g. multiple rows) -> flag and take first
    dup = d.groupby(["edb","yr","disc_yr"]).size().max()
    # original (as first disclosed) vs latest
    d = d.sort_values(["edb","yr","disc_yr"])
    first = d.groupby(["edb","yr"]).first().reset_index()
    last = d.groupby(["edb","yr"]).last().reset_index()  # includes basis of the value used
    m = last.merge(first[["edb","yr","value"]], on=["edb","yr"], suffixes=("","_orig"))
    m["restated"] = ~np.isclose(m.value, m.value_orig, rtol=1e-4, atol=0.5)
    m["metric"] = key
    rows.append(m[["edb","yr","metric","value","value_orig","restated","disc_yr","basis"]])
    audit.append((key,"ok" if dup==1 else f"dup rows x{dup}",len(m)))
long = pd.concat(rows)
long = long[long.yr >= 2013]  # no upper cap: latest year-end data flows through automatically
wide = long.pivot_table(index=["edb","yr"], columns="metric", values="value").reset_index()
# Combined series for amalgamated EDBs (see COMBINED in metrics_def.py)
for name, members, successor in COMBINED:
    sub = wide[wide.edb.isin(members)]
    out_rows = []
    for yr, g in sub.groupby("yr"):
        if set(g.edb) != set(members):      # post-merger: successor reports the combined business
            if (g.edb == successor).all() and not wide[(wide.edb.isin([m for m in members if m != successor])) & (wide.yr == yr)].shape[0]:
                r = g.iloc[0].copy(); r["edb"] = name; out_rows.append(r)
            continue
        r = pd.Series({"edb": name, "yr": yr})
        for k in [m[0] for m in METRICS]:
            rule = COMBINE_RULE.get(k)
            if k not in g or rule is None or g[k].isna().any(): r[k] = np.nan
            elif rule == "sum": r[k] = g[k].sum()
            else:
                w = g[rule[1]]
                r[k] = np.nan if w.isna().any() else (g[k] * w).sum() / w.sum()
        out_rows.append(r)
    if out_rows:
        wide = pd.concat([wide, pd.DataFrame(out_rows)], ignore_index=True)
        audit.append((f"combined: {name}", "ok", len(out_rows)))
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
