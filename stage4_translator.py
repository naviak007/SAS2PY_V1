"""
=============================================================
SAS to PySpark Accelerator — Stage 4: Translator
AST → PySpark Code Generator
Supports:
    DATA STEP
    PROC SORT
    PROC MEANS
    PROC SQL (AST)
=============================================================
"""

import logging

from stage3_parser import (
    LiteralNode,
    IdentifierNode,
    UnaryOpNode,
    BinaryOpNode,
    FunctionCallNode,
    InOpNode,
    BetweenOpNode,
    DataStepNode,
    ProcSortNode,
    ProcMeansNode,
    ProcSqlNode,
)

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stage4")


# =============================================================
# SAS → Spark Function Map
# =============================================================

FUNCTION_MAP = {
    "UPCASE": "upper",
    "LOWCASE": "lower",
    "ABS": "abs",
    "ROUND": "round",
}


# =============================================================
# Expression Generator
# =============================================================

def generate_expr(node, imports):

    if isinstance(node, LiteralNode):
        return repr(node.value)

    if isinstance(node, IdentifierNode):
        imports.add("from pyspark.sql.functions import col")
        return f'col("{node.name}")'

    if isinstance(node, UnaryOpNode):

        operand = generate_expr(node.operand, imports)

        if node.operator == "NOT":
            return f"(~{operand})"

        return f"(-{operand})"

    if isinstance(node, BinaryOpNode):

        left = generate_expr(node.left, imports)
        right = generate_expr(node.right, imports)

        OP_MAP = {
            "=": "==",
            "EQ": "==",
            "NE": "!=",
            "^=": "!=",
            "GT": ">",
            "LT": "<",
            "GE": ">=",
            "LE": "<=",
            "AND": "&",
            "OR": "|",
        }

        op = OP_MAP.get(node.operator, node.operator)

        return f"({left} {op} {right})"

    if isinstance(node, InOpNode):

        expr = generate_expr(node.expr, imports)
        values = ", ".join(generate_expr(v, imports) for v in node.values)

        return f"{expr}.isin({values})"

    if isinstance(node, BetweenOpNode):

        expr = generate_expr(node.expr, imports)
        low = generate_expr(node.low, imports)
        high = generate_expr(node.high, imports)

        return f"(({expr} >= {low}) & ({expr} <= {high}))"

    if isinstance(node, FunctionCallNode):

        name = node.name.upper()

        if name == "MISSING":
            expr = generate_expr(node.args[0], imports)
            return f"{expr}.isNull()"

        spark_fn = FUNCTION_MAP.get(name)

        if spark_fn:

            imports.add(f"from pyspark.sql.functions import {spark_fn}")

            args = ", ".join(generate_expr(a, imports) for a in node.args)

            return f"{spark_fn}({args})"

    raise ValueError(f"Unsupported expression node {node}")


# =============================================================
# Stage 4 Translator
# =============================================================

def run_stage4(ast_nodes):

    imports = set()
    sections = []

    for node in ast_nodes:

        # =====================================================
        # DATA STEP
        # =====================================================

        if isinstance(node, DataStepNode):

            out_df = f"df_{node.output_ds}"
            in_df = f"df_{node.input_ds}"

            lines = [f"{out_df} = {in_df}"]

            # WHERE

            for where_expr in node.where:

                expr = generate_expr(where_expr, imports)

                lines.append(
                    f"    .filter({expr})"
                )

            # Assignments

            for assign in node.assignments:

                rhs = generate_expr(assign.rhs, imports)

                lines.append(
                    f'    .withColumn("{assign.lhs}", {rhs})'
                )

            # IF / ELSE

            for if_node in node.if_then:

                cond = generate_expr(if_node.condition, imports)
                then_val = generate_expr(if_node.then_assign.rhs, imports)

                imports.add("from pyspark.sql.functions import when")

                if if_node.else_assign:

                    else_val = generate_expr(
                        if_node.else_assign.rhs,
                        imports
                    )

                    lines.append(
                        f'.withColumn("{if_node.then_assign.lhs}", '
                        f'when({cond}, {then_val}).otherwise({else_val}))'
                    )

                else:

                    lines.append(
                        f'.withColumn("{if_node.then_assign.lhs}", '
                        f'when({cond}, {then_val}).otherwise(None))'
                    )

            sections.append(" \\\n".join(lines))

        # =====================================================
        # PROC SORT
        # =====================================================

        elif isinstance(node, ProcSortNode):

            imports.add("from pyspark.sql.functions import col")

            out_df = f"df_{node.output_ds}"
            in_df = f"df_{node.input_ds}"

            order_expr = []

            for bv in node.by_vars:

                direction = ".desc()" if bv["desc"] else ".asc()"

                order_expr.append(
                    f'col("{bv["col"]}"){direction}'
                )

            code = (
                f"{out_df} = {in_df}.orderBy(\n"
                f"    {', '.join(order_expr)}\n"
                f")"
            )

            sections.append(code)

        # =====================================================
        # PROC MEANS
        # =====================================================

        elif isinstance(node, ProcMeansNode):

            imports.add("from pyspark.sql import functions as F")

            in_df = f"df_{node.input_ds}"
            out_df = "df_means_result"

            STAT_MAP = {
                "MEAN": "mean",
                "SUM": "sum",
                "MAX": "max",
                "MIN": "min",
                "STD": "stddev",
                "STDDEV": "stddev",
                "N": "count"
            }

            stats = node.stats or ["MEAN"]

            agg_list = []

            for stat in stats:

                spark_fn = STAT_MAP.get(stat)

                for var in node.stat_vars:

                    agg_list.append(
                        f'F.{spark_fn}("{var}").alias("{stat.lower()}_{var}")'
                    )

            agg_expr = ", ".join(agg_list)

            if node.class_vars:

                group_cols = ", ".join(f'"{c}"' for c in node.class_vars)

                code = (
                    f"{out_df} = (\n"
                    f"    {in_df}\n"
                    f"    .groupBy({group_cols})\n"
                    f"    .agg({agg_expr})\n"
                    f")"
                )

            else:

                code = (
                    f"{out_df} = (\n"
                    f"    {in_df}\n"
                    f"    .agg({agg_expr})\n"
                    f")"
                )

            sections.append(code)

        # =====================================================
        # PROC SQL (AST)
        # =====================================================

        elif isinstance(node, ProcSqlNode):

            imports.add("from pyspark.sql.functions import col")

            from_df = f"df_{node.from_node.table}"

            if node.from_node.alias:
                from_df += f'.alias("{node.from_node.alias}")'

            lines = []
            lines.append("df_sql_result = (")
            lines.append(f"    {from_df}")

            # JOIN

            if node.join:

                right_df = f"df_{node.join.table}"

                if node.join.alias:
                    right_df += f'.alias("{node.join.alias}")'

                lines.append(
                    f'    .join({right_df}, col("{node.join.left_col}") == col("{node.join.right_col}"))'
                )

            # WHERE

            if node.where:

                lines.append(
                    f'    .filter("{node.where.condition}")'
                )

            # SELECT

            if node.select.columns != ["*"]:

                cols = ", ".join(
                    f'col("{c}")' for c in node.select.columns
                )

                lines.append(
                    f"    .select({cols})"
                )

            lines.append(")")

            sections.append("\n".join(lines))

    final_code = "\n".join(sorted(imports)) + "\n\n" + "\n\n".join(sections)

    return final_code, 0


# =============================================================
# ENTRY POINT (Standalone Stage 4 Execution)
# =============================================================

if __name__ == "__main__":

    import sys
    from pathlib import Path

    from stage1_preprocessor import run_stage1
    from stage2_tokenizer import run_stage2
    from stage3_parser import run_stage3

    if len(sys.argv) != 2:

        print("\nUsage:")
        print("  python stage4_translator.py <input_file.sas>\n")
        sys.exit(1)

    input_sas = sys.argv[1]
    input_path = Path(input_sas)

    # Stage 1

    log.info("Running Stage 1...")
    stage1_result = run_stage1(input_sas)

    # Stage 2

    log.info("Running Stage 2...")
    token_map = run_stage2(stage1_result.blocks)

    # Stage 3

    log.info("Running Stage 3...")
    ast_nodes = run_stage3(token_map)

    # Stage 4

    log.info("Running Stage 4...")
    pyspark_code, todo_count = run_stage4(ast_nodes)

    print("\n" + "=" * 60)
    print("STAGE 4 OUTPUT — Raw PySpark Code")
    print("=" * 60 + "\n")

    print(pyspark_code)

    output_path = input_path.parent / f"{input_path.stem}_stage4_raw.py"

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(pyspark_code)

    log.info(f"Raw PySpark saved to: {output_path}")