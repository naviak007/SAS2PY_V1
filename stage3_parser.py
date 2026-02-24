"""
=============================================================
SAS to PySpark Accelerator — Stage 3: Parser
=============================================================
INPUT  : hr_report_stage1_cleaned.sas  (via Stage 2 token_map)
OUTPUT : List of AST nodes  →  hr_report_stage3_ast.txt

Run order:
  stage1_preprocessor.py  →  hr_report_stage1_cleaned.sas
  stage2_tokenizer.py     →  token_map (in memory)
  stage3_parser.py        →  AST nodes  →  hr_report_stage3_ast.txt

Sub-steps:
  1. Define AST node dataclasses
  2. Implement Parser class with peek() / eat() helpers
  3. parse() top-level dispatcher
  4. parse_data_step()
  5. parse_proc_sort()
  6. parse_proc_means()
  7. parse_proc_sql()
  8. Return AST node list to Stage 4
=============================================================
"""

import os
import re
import sys
import logging
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("stage3")

DOWNLOADS = r"C:\Users\MelissaSebastian\Downloads"
sys.path.insert(0, DOWNLOADS)

from stage2_tokenizer import TokenType, Token, run_stage2


# ══════════════════════════════════════════════════════════════
# STEP 1 — AST Node Dataclasses
# ══════════════════════════════════════════════════════════════

@dataclass
class ExprNode:
    """A raw expression string (RHS of assignment, WHERE condition, etc.)"""
    expr: str

    def __repr__(self):
        return f"ExprNode('{self.expr}')"


@dataclass
class AssignNode:
    """col = expression"""
    lhs: str        # column being assigned
    rhs: ExprNode   # right-hand side expression


@dataclass
class IfThenNode:
    """IF condition THEN col=val [ELSE col=val]"""
    condition:   str
    then_assign: Optional[AssignNode] = None
    else_assign: Optional[AssignNode] = None


@dataclass
class DataStepNode:
    """DATA output; SET input; ... RUN;"""
    output_ds:   str
    input_ds:    str
    where:       List[str]           = field(default_factory=list)
    keep:        List[str]           = field(default_factory=list)
    drop:        List[str]           = field(default_factory=list)
    rename:      Dict[str, str]      = field(default_factory=dict)
    assignments: List[AssignNode]    = field(default_factory=list)
    if_then:     List[IfThenNode]    = field(default_factory=list)


@dataclass
class ProcSortNode:
    """PROC SORT DATA=... OUT=... BY ... RUN;"""
    input_ds:  str
    output_ds: str
    by_vars:   List[Dict]  = field(default_factory=list)   # [{col, desc}]
    nodupkey:  bool        = False


@dataclass
class ProcMeansNode:
    """PROC MEANS DATA=... CLASS ... VAR ... OUTPUT OUT=... RUN;"""
    input_ds:   str
    output_ds:  str                       = ""
    class_vars: List[str]                 = field(default_factory=list)
    stat_vars:  List[str]                 = field(default_factory=list)
    stats:      Dict[str, List[str]]      = field(default_factory=dict)


@dataclass
class ProcSqlNode:
    """PROC SQL; CREATE TABLE ... AS SELECT ... QUIT;"""
    create_table: str
    raw_sql:      str   # full SELECT preserved for spark.sql()


# ══════════════════════════════════════════════════════════════
# STEP 2 — Parser class  (peek / eat helpers)
# ══════════════════════════════════════════════════════════════

class ParseError(Exception):
    pass


class Parser:
    """
    Recursive-descent parser.
    Holds a flat token list and a position pointer (self.pos).
    """

    def __init__(self, tokens: List[Token], block_type: str = ""):
        self.tokens     = tokens
        self.pos        = 0
        self.block_type = block_type

    # ── Helpers ───────────────────────────────────────────────

    def peek(self) -> Token:
        """Return current token WITHOUT consuming it."""
        if self.pos < len(self.tokens):
            return self.tokens[self.pos]
        return Token(TokenType.EOF, None, -1)

    def peek_next(self) -> Token:
        """Look one token ahead without consuming."""
        if self.pos + 1 < len(self.tokens):
            return self.tokens[self.pos + 1]
        return Token(TokenType.EOF, None, -1)

    def eat(self, expected_type=None, expected_value=None) -> Token:
        """
        Consume and return current token.
        Raises ParseError if type or value does not match.
        """
        tok = self.peek()
        if tok.type == TokenType.EOF:
            raise ParseError(f"Unexpected EOF at line {tok.line}")
        if expected_type and tok.type != expected_type:
            raise ParseError(
                f"Expected {expected_type} but got "
                f"{tok.type}({tok.value!r}) at line {tok.line}"
            )
        if expected_value and str(tok.value).upper() != str(expected_value).upper():
            raise ParseError(
                f"Expected '{expected_value}' but got '{tok.value}' "
                f"at line {tok.line}"
            )
        self.pos += 1
        return tok

    def eat_if(self, expected_type=None, expected_value=None) -> Optional[Token]:
        """Consume token only if it matches — no error if it doesn't."""
        tok = self.peek()
        if expected_type and tok.type != expected_type:
            return None
        if expected_value and str(tok.value).upper() != str(expected_value).upper():
            return None
        self.pos += 1
        return tok

    def collect_until_punct(self, punct: str = ";") -> str:
        """Collect all tokens as a string until a punctuation char."""
        parts = []
        while True:
            tok = self.peek()
            if tok.type == TokenType.EOF:
                break
            if tok.type == TokenType.PUNCT and tok.value == punct:
                self.eat()
                break
            self.pos += 1
            val = f"'{tok.value}'" if tok.type == TokenType.LITERAL_STR \
                  else str(tok.value) if tok.value is not None else ""
            parts.append(val)
        return " ".join(parts).strip()

    def collect_idents_until_semi(self) -> List[str]:
        """Collect IDENTIFIER/KEYWORD values into a list until ;"""
        names = []
        while True:
            tok = self.peek()
            if tok.type == TokenType.EOF:
                break
            if tok.type == TokenType.PUNCT and tok.value == ";":
                self.eat()
                break
            if tok.type in (TokenType.IDENTIFIER, TokenType.KEYWORD):
                names.append(str(tok.value))
            self.pos += 1
        return names

    # ── STEP 3: top-level dispatcher ──────────────────────────

    def parse(self):
        """
        Peek at first token → dispatch to parse_data_step,
        parse_proc_sort, parse_proc_means, parse_proc_sql.
        """
        tok = self.peek()
        if tok.type != TokenType.KEYWORD:
            raise ParseError(f"Expected keyword at start of block, got {tok}")

        kw = tok.value.upper()

        if kw == "DATA":
            return self.parse_data_step()

        if kw == "PROC":
            proc_name = str(self.peek_next().value).upper()
            if proc_name == "SORT":
                return self.parse_proc_sort()
            elif proc_name in ("MEANS", "SUMMARY"):
                return self.parse_proc_means()
            elif proc_name == "SQL":
                return self.parse_proc_sql()
            else:
                log.warning(f"  Unsupported PROC: {proc_name} — flagged as TODO")
                return None

        log.warning(f"  Unknown block keyword: {kw}")
        return None

    # ── STEP 4: parse_data_step() ─────────────────────────────

    def parse_data_step(self) -> DataStepNode:
        """
        DATA output_ds;
          SET input_ds;
          [WHERE ...;]
          [KEEP ...;]
          [DROP ...;]
          [RENAME old=new;]
          [col = expr;]
          [IF cond THEN col=val; ELSE col=val;]
        RUN;
        """
        self.eat(TokenType.KEYWORD, "DATA")
        output_ds = str(self.eat(TokenType.IDENTIFIER).value)
        self.eat(TokenType.PUNCT, ";")

        self.eat(TokenType.KEYWORD, "SET")
        input_ds = str(self.eat(TokenType.IDENTIFIER).value)
        self.eat(TokenType.PUNCT, ";")

        node = DataStepNode(output_ds=output_ds, input_ds=input_ds)

        while True:
            tok = self.peek()

            # End of DATA step
            if tok.type == TokenType.EOF:
                break
            if tok.type == TokenType.KEYWORD and tok.value == "RUN":
                self.eat()
                self.eat_if(TokenType.PUNCT, ";")
                break

            # WHERE condition;
            if tok.type == TokenType.KEYWORD and tok.value == "WHERE":
                self.eat()
                node.where.append(self.collect_until_punct(";"))

            # KEEP col1 col2 ...;
            elif tok.type == TokenType.KEYWORD and tok.value == "KEEP":
                self.eat()
                node.keep = self.collect_idents_until_semi()

            # DROP col1 ...;
            elif tok.type == TokenType.KEYWORD and tok.value == "DROP":
                self.eat()
                node.drop = self.collect_idents_until_semi()

            # RENAME old=new ...;
            elif tok.type == TokenType.KEYWORD and tok.value == "RENAME":
                self.eat()
                while True:
                    t = self.peek()
                    if t.type == TokenType.PUNCT and t.value == ";":
                        self.eat()
                        break
                    if t.type == TokenType.EOF:
                        break
                    if t.type == TokenType.IDENTIFIER:
                        old = str(self.eat().value)
                        self.eat_if(TokenType.OPERATOR, "=")
                        new_tok = self.peek()
                        if new_tok.type == TokenType.IDENTIFIER:
                            node.rename[old] = str(self.eat().value)
                    else:
                        self.pos += 1

            # IF cond THEN ...
            elif tok.type == TokenType.KEYWORD and tok.value == "IF":
                if_node = self._parse_if_then()
                if if_node:
                    node.if_then.append(if_node)

            # Skip bare ELSE (already handled inside _parse_if_then)
            elif tok.type == TokenType.KEYWORD and tok.value == "ELSE":
                self.pos += 1

            # Assignment: identifier = expr;
            elif tok.type == TokenType.IDENTIFIER:
                nxt = self.peek_next()
                if nxt.type == TokenType.OPERATOR and nxt.value == "=":
                    lhs = str(self.eat().value)
                    self.eat()   # consume =
                    rhs_str = self.collect_until_punct(";")
                    node.assignments.append(
                        AssignNode(lhs=lhs, rhs=ExprNode(rhs_str))
                    )
                else:
                    self.pos += 1

            else:
                self.pos += 1   # skip unrecognised token

        return node

    def _parse_if_then(self) -> Optional[IfThenNode]:
        """IF condition THEN col=val; [ELSE IF ... | ELSE col=val;]"""
        self.eat(TokenType.KEYWORD, "IF")

        # Collect condition tokens up to THEN
        cond_parts = []
        while True:
            t = self.peek()
            if t.type == TokenType.EOF:
                break
            if t.type == TokenType.KEYWORD and t.value == "THEN":
                break
            self.pos += 1
            val = f"'{t.value}'" if t.type == TokenType.LITERAL_STR \
                  else str(t.value) if t.value is not None else ""
            cond_parts.append(val)
        condition = " ".join(cond_parts).strip()
        self.eat(TokenType.KEYWORD, "THEN")

        # Parse THEN assignment
        then_assign = None
        t = self.peek()
        if t.type == TokenType.IDENTIFIER:
            nxt = self.peek_next()
            if nxt.type == TokenType.OPERATOR and nxt.value == "=":
                lhs = str(self.eat().value)
                self.eat()   # =
                rhs_str = self.collect_until_punct(";")
                then_assign = AssignNode(lhs=lhs, rhs=ExprNode(rhs_str))

        return IfThenNode(condition=condition, then_assign=then_assign)

    # ── STEP 5: parse_proc_sort() ─────────────────────────────

    def parse_proc_sort(self) -> ProcSortNode:
        """
        PROC SORT DATA=input [OUT=output] [NODUPKEY];
          BY [DESCENDING] col ...;
        RUN;
        """
        self.eat(TokenType.KEYWORD, "PROC")
        self.eat(TokenType.KEYWORD, "SORT")

        input_ds  = ""
        output_ds = ""
        nodupkey  = False

        # Parse header options until ;
        while True:
            tok = self.peek()
            if tok.type == TokenType.PUNCT and tok.value == ";":
                self.eat()
                break
            if tok.type == TokenType.EOF:
                break
            if tok.type == TokenType.KEYWORD and tok.value == "DATA":
                self.eat()
                self.eat_if(TokenType.OPERATOR, "=")
                input_ds = str(self.eat(TokenType.IDENTIFIER).value)
            elif tok.type == TokenType.KEYWORD and tok.value == "OUT":
                self.eat()
                self.eat_if(TokenType.OPERATOR, "=")
                output_ds = str(self.eat(TokenType.IDENTIFIER).value)
            elif tok.type == TokenType.KEYWORD and tok.value == "NODUPKEY":
                nodupkey = True
                self.eat()
            else:
                self.pos += 1

        node = ProcSortNode(
            input_ds=input_ds,
            output_ds=output_ds or input_ds,
            nodupkey=nodupkey
        )

        # BY col [DESCENDING col] ...;
        if self.eat_if(TokenType.KEYWORD, "BY"):
            desc_next = False
            while True:
                tok = self.peek()
                if tok.type == TokenType.PUNCT and tok.value == ";":
                    self.eat()
                    break
                if tok.type == TokenType.EOF:
                    break
                if tok.type == TokenType.KEYWORD and tok.value == "DESCENDING":
                    desc_next = True
                    self.eat()
                    continue
                if tok.type == TokenType.IDENTIFIER:
                    node.by_vars.append({"col": str(tok.value), "desc": desc_next})
                    desc_next = False
                self.pos += 1

        self.eat_if(TokenType.KEYWORD, "RUN")
        self.eat_if(TokenType.PUNCT, ";")
        return node

    # ── STEP 6: parse_proc_means() ────────────────────────────

    def parse_proc_means(self) -> ProcMeansNode:
        """
        PROC MEANS DATA=input [NOPRINT];
          CLASS col;
          VAR col1 col2;
          OUTPUT OUT=output MEAN=m1 m2 MAX=x1 x2 ...;
        RUN;
        """
        self.eat(TokenType.KEYWORD, "PROC")
        self.eat(TokenType.KEYWORD, "MEANS")

        input_ds = ""
        # Parse header options until ;
        while True:
            tok = self.peek()
            if tok.type == TokenType.PUNCT and tok.value == ";":
                self.eat()
                break
            if tok.type == TokenType.EOF:
                break
            if tok.type == TokenType.KEYWORD and tok.value == "DATA":
                self.eat()
                self.eat_if(TokenType.OPERATOR, "=")
                input_ds = str(self.eat(TokenType.IDENTIFIER).value)
            else:
                self.pos += 1

        node = ProcMeansNode(input_ds=input_ds)

        # Parse CLASS / VAR / OUTPUT statements until RUN
        while True:
            tok = self.peek()
            if tok.type == TokenType.EOF:
                break
            if tok.type == TokenType.KEYWORD and tok.value == "RUN":
                self.eat()
                self.eat_if(TokenType.PUNCT, ";")
                break

            if tok.type == TokenType.KEYWORD and tok.value == "CLASS":
                self.eat()
                node.class_vars = self.collect_idents_until_semi()

            elif tok.type == TokenType.KEYWORD and tok.value == "VAR":
                self.eat()
                node.stat_vars = self.collect_idents_until_semi()

            elif tok.type == TokenType.KEYWORD and tok.value == "OUTPUT":
                self.eat()
                current_stat = None
                while True:
                    t = self.peek()
                    if t.type == TokenType.PUNCT and t.value == ";":
                        self.eat()
                        break
                    if t.type == TokenType.EOF:
                        break
                    if t.type == TokenType.KEYWORD and t.value == "OUT":
                        self.eat()
                        self.eat_if(TokenType.OPERATOR, "=")
                        node.output_ds = str(self.eat(TokenType.IDENTIFIER).value)
                    elif t.type == TokenType.KEYWORD and t.value in (
                            "MEAN", "MAX", "MIN", "STD", "N", "SUM"):
                        current_stat = t.value.lower()
                        node.stats[current_stat] = []
                        self.eat()
                        self.eat_if(TokenType.OPERATOR, "=")
                    elif t.type == TokenType.IDENTIFIER and current_stat is not None:
                        node.stats[current_stat].append(str(t.value))
                        self.eat()
                    else:
                        self.pos += 1
            else:
                self.pos += 1

        return node

    # ── STEP 7: parse_proc_sql() ──────────────────────────────

    def parse_proc_sql(self) -> ProcSqlNode:
        """
        PROC SQL;
          CREATE TABLE output AS
          SELECT ... FROM ... ;
        QUIT;

        The raw SQL is preserved as-is for spark.sql().
        """
        self.eat(TokenType.KEYWORD, "PROC")
        self.eat(TokenType.KEYWORD, "SQL")
        self.eat(TokenType.PUNCT, ";")

        create_table = ""
        sql_parts    = []

        while True:
            tok = self.peek()
            if tok.type == TokenType.EOF:
                break
            if tok.type == TokenType.KEYWORD and tok.value == "QUIT":
                self.eat()
                self.eat_if(TokenType.PUNCT, ";")
                break

            # Capture CREATE TABLE <name> AS
            if tok.type == TokenType.KEYWORD and tok.value == "CREATE":
                self.eat()                            # CREATE
                self.eat_if(TokenType.KEYWORD, "TABLE")
                create_table = str(self.eat(TokenType.IDENTIFIER).value)
                self.eat_if(TokenType.KEYWORD, "AS")
                sql_parts.append(f"CREATE TABLE {create_table} AS")
            else:
                # Reconstruct SQL token by token
                if tok.type == TokenType.LITERAL_STR:
                    sql_parts.append(f"'{tok.value}'")
                elif tok.type == TokenType.PUNCT and tok.value == ";":
                    sql_parts.append(";")
                    self.pos += 1
                    continue
                else:
                    sql_parts.append(str(tok.value) if tok.value is not None else "")
                self.pos += 1

        raw_sql = " ".join(sql_parts).strip()

        # Extract just the SELECT ... part (drop CREATE TABLE x AS prefix)
        m = re.search(r"\bSELECT\b", raw_sql, re.IGNORECASE)
        select_sql = raw_sql[m.start():] if m else raw_sql

        return ProcSqlNode(create_table=create_table, raw_sql=select_sql)


# ══════════════════════════════════════════════════════════════
# STEP 8 — run_stage3()  (orchestrator)
# ══════════════════════════════════════════════════════════════

def run_stage3(token_map: dict) -> List:
    """
    Parse every block's token list into an AST node.

    Args:
        token_map : dict from run_stage2()
                    { block_index: [Token, Token, ...] }

    Returns:
        List of AST node objects (one per block)
    """
    log.info("=" * 55)
    log.info("STAGE 3 — Parser")
    log.info("=" * 55)

    ast_nodes = []

    for block_idx, tokens in token_map.items():

        # Get block_type from first token if available
        if tokens and hasattr(tokens[0], "block_type"):
            block_type = tokens[0].block_type
        else:
            block_type = "UNKNOWN"

        log.info(f"  Parsing Block {block_idx}: {block_type}")

        try:
            parser = Parser(tokens, block_type)
            node   = parser.parse()

            if node is not None:
                ast_nodes.append(node)
                log.info(f"  [Block {block_idx}] → {type(node).__name__}")
            else:
                log.warning(f"  [Block {block_idx}] No AST node — skipped")

        except ParseError as e:
            log.error(f"  [Block {block_idx}] ParseError: {e}")

    log.info("=" * 55)
    log.info(f"Stage 3 complete  |  {len(ast_nodes)} AST nodes")
    log.info("=" * 55)

    return ast_nodes


# ══════════════════════════════════════════════════════════════
# Pretty printer + saver
# ══════════════════════════════════════════════════════════════

def print_ast(ast_nodes: List):
    print(f"\n{'='*60}")
    print("  STAGE 3 RESULTS — Abstract Syntax Tree")
    print(f"{'='*60}")

    for i, node in enumerate(ast_nodes, 1):
        print(f"\n[Node {i}]  {type(node).__name__}")
        print(f"  {'-'*56}")

        if isinstance(node, DataStepNode):
            print(f"  output_ds   : {node.output_ds}")
            print(f"  input_ds    : {node.input_ds}")
            print(f"  where       : {node.where}")
            print(f"  keep        : {node.keep}")
            print(f"  drop        : {node.drop}")
            print(f"  rename      : {node.rename}")
            print(f"  assignments :")
            for a in node.assignments:
                print(f"    {a.lhs}  =  {a.rhs.expr}")
            print(f"  if_then     :")
            for it in node.if_then:
                ta = f"{it.then_assign.lhs} = {it.then_assign.rhs.expr}" \
                     if it.then_assign else "None"
                print(f"    IF {it.condition}  THEN  {ta}")

        elif isinstance(node, ProcSortNode):
            print(f"  input_ds    : {node.input_ds}")
            print(f"  output_ds   : {node.output_ds}")
            print(f"  by_vars     : {node.by_vars}")
            print(f"  nodupkey    : {node.nodupkey}")

        elif isinstance(node, ProcMeansNode):
            print(f"  input_ds    : {node.input_ds}")
            print(f"  output_ds   : {node.output_ds}")
            print(f"  class_vars  : {node.class_vars}")
            print(f"  stat_vars   : {node.stat_vars}")
            print(f"  stats       : {node.stats}")

        elif isinstance(node, ProcSqlNode):
            print(f"  create_table: {node.create_table}")
            print(f"  raw_sql     :")
            for line in node.raw_sql.split():
                pass
            print(f"    {node.raw_sql[:200]}...")

    print(f"\n{'='*60}\n")


def save_ast(ast_nodes: List, out_filepath: str):
    with open(out_filepath, "w", encoding="utf-8") as f:
        f.write("SAS to PySpark Accelerator — Stage 3 AST\n")
        f.write("=" * 60 + "\n")
        for i, node in enumerate(ast_nodes, 1):
            f.write(f"\n[Node {i}]  {type(node).__name__}\n")
            f.write(f"  {repr(node)}\n")
    log.info(f"AST saved to: {out_filepath}")


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    STAGE1_CLEANED = os.path.join(DOWNLOADS, "hr_report_stage1_cleaned.sas")

    log.info("Running Stage 2 to get tokens...")
    token_map = run_stage2(STAGE1_CLEANED)

    ast_nodes = run_stage3(token_map)
    print_ast(ast_nodes)

    out_path = os.path.join(DOWNLOADS, "hr_report_stage3_ast.txt")
    save_ast(ast_nodes, out_path)
