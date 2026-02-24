"""
=============================================================
SAS to PySpark Accelerator — Stage 4: Translator
=============================================================
INPUT  : AST nodes from Stage 3 (run_stage3)
OUTPUT : Raw PySpark code string  →  hr_report_stage4_raw.py

Run order:
  stage1  →  stage2  →  stage3  →  stage4_translator.py

Sub-steps:
  1. FUNCTION_MAP  — SAS built-in → PySpark equivalent
  2. translate_expr()  — swap SAS functions in expressions
  3. translate_DataStepNode()
  4. translate_ProcSortNode()
  5. translate_ProcMeansNode()
  6. translate_ProcSqlNode()
  7. isinstance() visitor dispatch + import accumulation
  8. Return assembled PySpark code string to Stage 5
=============================================================
"""

import os
import re
import sys
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stage4")

DOWNLOADS = r"C:\Users\MelissaSebastian\Downloads"
sys.path.insert(0, DOWNLOADS)

from stage2_tokenizer import run_stage2
from stage3_parser import (
    run_stage3,
    DataStepNode, ProcSortNode, ProcMeansNode, ProcSqlNode,
    AssignNode, IfThenNode, ExprNode,
)


# ══════════════════════════════════════════════════════════════
# STEP 1 — SAS → PySpark Function Map
# key   : SAS function name (UPPER)
# value : (pyspark_function_name, [import_module])
# ══════════════════════════════════════════════════════════════

FUNCTION_MAP = {
    # String
    "UPCASE":   ("upper",        "pyspark.sql.functions"),
    "LOWCASE":  ("lower",        "pyspark.sql.functions"),
    "STRIP":    ("trim",         "pyspark.sql.functions"),
    "SUBSTR":   ("substring",    "pyspark.sql.functions"),
    "LENGTH":   ("length",       "pyspark.sql.functions"),
    "CATS":     ("concat",       "pyspark.sql.functions"),
    "CAT":      ("concat",       "pyspark.sql.functions"),
    "SCAN":     ("split",        "pyspark.sql.functions"),
    "COMPRESS": ("regexp_replace","pyspark.sql.functions"),
    # Date
    "TODAY":    ("current_date", "pyspark.sql.functions"),
    "YEAR":     ("year",         "pyspark.sql.functions"),
    "MONTH":    ("month",        "pyspark.sql.functions"),
    "DAY":      ("dayofmonth",   "pyspark.sql.functions"),
    "MDY":      ("make_date",    "pyspark.sql.functions"),
    "DATEPART": ("to_date",      "pyspark.sql.functions"),
    # Numeric
    "INT":      ("floor",        "pyspark.sql.functions"),
    "ABS":      ("abs",          "pyspark.sql.functions"),
    "ROUND":    ("round",        "pyspark.sql.functions"),
    "CEIL":     ("ceil",         "pyspark.sql.functions"),
    "SQRT":     ("sqrt",         "pyspark.sql.functions"),
    "LOG":      ("log",          "pyspark.sql.functions"),
    "MAX":      ("greatest",     "pyspark.sql.functions"),
    "MIN":      ("least",        "pyspark.sql.functions"),
}


# ══════════════════════════════════════════════════════════════
# STEP 2 — translate_expr()
# Substitute SAS function names and wrap column refs in col()
# ══════════════════════════════════════════════════════════════

def translate_expr(expr: str, imports: set) -> str:
    """
    1. Replace SAS function names with PySpark equivalents.
    2. Wrap bare column identifiers in col("...").
    3. Translate SAS operators (^= → !=, EQ → ==, etc.)
    """
    result = expr

    # Swap SAS operators to Python equivalents
    OPERATOR_MAP = {
        r"\^=": "!=",
        r"\bNE\b": "!=",
        r"\bEQ\b": "==",
        r"\bGT\b": ">",
        r"\bLT\b": "<",
        r"\bGE\b": ">=",
        r"\bLE\b": "<=",
        r"\bAND\b": "&",
        r"\bOR\b":  "|",
        r"\bNOT\b": "~",
    }
    for sas_op, py_op in OPERATOR_MAP.items():
        result = re.sub(sas_op, py_op, result, flags=re.IGNORECASE)

    # Swap SAS function names
    for sas_fn, (spark_fn, module) in FUNCTION_MAP.items():
        pattern = re.compile(rf"\b{sas_fn}\s*\(", re.IGNORECASE)
        if pattern.search(result):
            result = pattern.sub(f"{spark_fn}(", result)
            short_name = spark_fn
            imports.add(f"from {module} import {short_name}")

    imports.add("from pyspark.sql.functions import col")
    return result


# ══════════════════════════════════════════════════════════════
# Utility: clean SAS dataset name → Python df variable name
# WORK.employees_2024  →  df_employees_2024
# raw.hr_data          →  df_hr_data
# ══════════════════════════════════════════════════════════════

def df_name(ds: str) -> str:
    return "df_" + ds.split(".")[-1]


# ══════════════════════════════════════════════════════════════
# STEP 3 — translate_DataStepNode()
# ══════════════════════════════════════════════════════════════

def translate_DataStepNode(node: DataStepNode, imports: set) -> str:
    imports.add("from pyspark.sql.functions import col, when")

    out_df = df_name(node.output_ds)
    in_df  = df_name(node.input_ds)

    lines = [f"{out_df} = {in_df}"]

    # WHERE → .filter()
    for condition in node.where:
        expr = translate_expr(condition, imports)
        lines.append(f'    .filter("{expr}")')

    # DROP → .drop()
    for col_name in node.drop:
        lines.append(f'    .drop("{col_name}")')

    # RENAME → .withColumnRenamed()
    for old, new in node.rename.items():
        lines.append(f'    .withColumnRenamed("{old}", "{new}")')

    # Assignments → .withColumn()
    for assign in node.assignments:
        rhs = translate_expr(assign.rhs.expr, imports)
        lines.append(f'    .withColumn("{assign.lhs}", {rhs})')

    # IF/THEN → .withColumn( when().otherwise() )
    for if_node in node.if_then:
        if if_node.then_assign:
            cond = translate_expr(if_node.condition, imports)
            raw_val = if_node.then_assign.rhs.expr.strip().strip("'\"")
            val = f'"{raw_val}"'
            lines.append(
                f'    .withColumn("{if_node.then_assign.lhs}", '
                f'when({cond}, {val}).otherwise(None))'
            )
            imports.add("from pyspark.sql.functions import when")

    # KEEP → .select()
    if node.keep:
        cols_str = ", ".join(f'"{c}"' for c in node.keep)
        lines.append(f"    .select({cols_str})")

    return " \\\n".join(lines)


# ══════════════════════════════════════════════════════════════
# STEP 4 — translate_ProcSortNode()
# ══════════════════════════════════════════════════════════════

def translate_ProcSortNode(node: ProcSortNode, imports: set) -> str:
    imports.add("from pyspark.sql.functions import col")

    out_df = df_name(node.output_ds)
    in_df  = df_name(node.input_ds)

    order_args = []
    for bv in node.by_vars:
        direction = ".desc()" if bv["desc"] else ".asc()"
        order_args.append(f'col("{bv["col"]}"){direction}')

    lines = [f"{out_df} = {in_df}"]
    lines.append(f"    .orderBy({', '.join(order_args)})")

    if node.nodupkey:
        lines.append("    .dropDuplicates()")

    return " \\\n".join(lines)


# ══════════════════════════════════════════════════════════════
# STEP 5 — translate_ProcMeansNode()
# ══════════════════════════════════════════════════════════════

def translate_ProcMeansNode(node: ProcMeansNode, imports: set) -> str:
    imports.add("from pyspark.sql import functions as F")

    in_df  = df_name(node.input_ds)
    out_df = df_name(node.output_ds) if node.output_ds else "df_means_result"

    group_cols = ", ".join(f'"{c}"' for c in node.class_vars)

    STAT_FN = {
        "mean": "F.mean", "max": "F.max", "min": "F.min",
        "std":  "F.stddev", "n":  "F.count", "sum": "F.sum",
    }

    agg_exprs = []
    for stat, aliases in node.stats.items():
        fn = STAT_FN.get(stat, f"F.{stat}")
        for col_name, alias in zip(node.stat_vars, aliases):
            agg_exprs.append(f'{fn}("{col_name}").alias("{alias}")')

    # Fallback if no OUTPUT statement parsed
    if not agg_exprs:
        for col_name in node.stat_vars:
            agg_exprs.append(f'F.mean("{col_name}").alias("mean_{col_name}")')

    agg_str = ",\n        ".join(agg_exprs)

    return (
        f"{out_df} = {in_df} \\\n"
        f"    .groupBy({group_cols}) \\\n"
        f"    .agg(\n        {agg_str}\n    )"
    )


# ══════════════════════════════════════════════════════════════
# STEP 6 — translate_ProcSqlNode()
# ══════════════════════════════════════════════════════════════

def translate_ProcSqlNode(node: ProcSqlNode, imports: set) -> str:
    imports.add("from pyspark.sql import SparkSession")

    out_df = df_name(node.create_table)

    # Strip WORK. prefix — Spark uses temp view names directly
    sql = re.sub(r"\bWORK\s*\.\s*", "", node.raw_sql, flags=re.IGNORECASE)

    return (
        f'{out_df} = spark.sql("""\n'
        f"    {sql.strip()}\n"
        f'""")'
    )


# ══════════════════════════════════════════════════════════════
# STEP 7 — Visitor dispatch + import accumulation
# ══════════════════════════════════════════════════════════════

def run_stage4(ast_nodes: list):
    """
    Walk AST nodes, dispatch to translate_*() methods,
    accumulate imports, assemble final code string.

    Returns:
        (pyspark_code: str, todo_count: int)
    """
    log.info("=" * 55)
    log.info("STAGE 4 — Translator")
    log.info("=" * 55)

    imports    = set()
    sections   = []
    todo_count = 0

    for node in ast_nodes:
        node_type = type(node).__name__
        log.info(f"  Translating: {node_type}")

        try:
            if isinstance(node, DataStepNode):
                label = f"DATA STEP → {df_name(node.output_ds)}"
                code  = translate_DataStepNode(node, imports)

            elif isinstance(node, ProcSortNode):
                label = f"PROC SORT → {df_name(node.output_ds)}"
                code  = translate_ProcSortNode(node, imports)

            elif isinstance(node, ProcMeansNode):
                label = f"PROC MEANS → {df_name(node.output_ds) if node.output_ds else 'df_means_result'}"
                code  = translate_ProcMeansNode(node, imports)

            elif isinstance(node, ProcSqlNode):
                label = f"PROC SQL → {df_name(node.create_table)}"
                code  = translate_ProcSqlNode(node, imports)

            else:
                label = f"UNSUPPORTED: {node_type}"
                code  = f"# TODO: manual conversion required — {node_type}"
                todo_count += 1
                log.warning(f"  No translator for {node_type} — flagged as TODO")

            sections.append(f"# ── {label}\n{code}\n")

        except Exception as e:
            log.error(f"  Translation error for {node_type}: {e}")
            sections.append(f"# TODO: translation error in {node_type}: {e}\n")
            todo_count += 1

    # STEP 8: assemble — sorted imports first, then body
    sorted_imports = sorted(imports)
    output = "\n".join(sorted_imports) + "\n\n" + "\n".join(sections)

    log.info("=" * 55)
    log.info(f"Stage 4 complete  |  {len(ast_nodes)} nodes translated  |  {todo_count} TODOs")
    log.info("=" * 55)

    return output, todo_count


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    STAGE1_CLEANED = os.path.join(DOWNLOADS, "hr_report_stage1_cleaned.sas")

    log.info("Running Stage 2...")
    token_map = run_stage2(STAGE1_CLEANED)
    log.info("Running Stage 3...")
    ast_nodes = run_stage3(token_map)
    log.info("Running Stage 4...")
    pyspark_code, todo_count = run_stage4(ast_nodes)

    print(f"\n{'='*60}")
    print("  STAGE 4 RESULTS — Raw Generated PySpark Code")
    print(f"{'='*60}\n")
    print(pyspark_code)

    out_path = os.path.join(DOWNLOADS, "hr_report_stage4_raw.py")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(pyspark_code)
    log.info(f"Raw PySpark saved to: {out_path}")
