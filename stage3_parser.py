"""
=============================================================
SAS to PySpark Accelerator — Stage 3: Parser
Expression AST + DATA + PROC support + SQL AST
=============================================================
"""

import logging
import re
from dataclasses import dataclass, field
from typing import List, Optional

from stage2_tokenizer import TokenType, Token

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("stage3")


# =============================================================
# Expression AST
# =============================================================

class ExprNode:
    pass


@dataclass
class LiteralNode(ExprNode):
    value: any


@dataclass
class IdentifierNode(ExprNode):
    name: str


@dataclass
class UnaryOpNode(ExprNode):
    operator: str
    operand: ExprNode


@dataclass
class BinaryOpNode(ExprNode):
    left: ExprNode
    operator: str
    right: ExprNode


@dataclass
class FunctionCallNode(ExprNode):
    name: str
    args: List[ExprNode]


@dataclass
class InOpNode(ExprNode):
    expr: ExprNode
    values: List[ExprNode]


@dataclass
class BetweenOpNode(ExprNode):
    expr: ExprNode
    low: ExprNode
    high: ExprNode


# =============================================================
# DATA STEP AST
# =============================================================

@dataclass
class AssignNode:
    lhs: str
    rhs: ExprNode


@dataclass
class IfThenNode:
    condition: ExprNode
    then_assign: AssignNode
    else_assign: Optional[AssignNode] = None


@dataclass
class DataStepNode:
    output_ds: str
    input_ds: str
    where: List[ExprNode] = field(default_factory=list)
    assignments: List[AssignNode] = field(default_factory=list)
    if_then: List[IfThenNode] = field(default_factory=list)


# =============================================================
# PROC AST
# =============================================================

@dataclass
class ProcSortNode:
    input_ds: str
    output_ds: str
    by_vars: List[dict]


@dataclass
class ProcMeansNode:
    input_ds: str
    class_vars: List[str]
    stat_vars: List[str]
    stats: List[str] = field(default_factory=list)


# =============================================================
# SQL AST
# =============================================================

@dataclass
class SqlSelectNode:
    columns: List[str]


@dataclass
class SqlFromNode:
    table: str
    alias: Optional[str] = None


@dataclass
class SqlJoinNode:
    table: str
    alias: Optional[str]
    left_col: str
    right_col: str


@dataclass
class SqlWhereNode:
    condition: str


@dataclass
class ProcSqlNode:
    select: SqlSelectNode
    from_node: SqlFromNode
    join: Optional[SqlJoinNode] = None
    where: Optional[SqlWhereNode] = None


# =============================================================
# Parser
# =============================================================

class ParseError(Exception):
    pass


class Parser:

    def __init__(self, tokens: List[Token]):
        self.tokens = tokens
        self.pos = 0

    def peek(self):
        return self.tokens[self.pos]

    def eat(self, expected_type=None, expected_value=None):

        tok = self.peek()

        if expected_type and tok.type != expected_type:
            raise ParseError(f"Expected {expected_type}, got {tok.type}")

        if expected_value and str(tok.value).upper() != expected_value:
            raise ParseError(f"Expected {expected_value}, got {tok.value}")

        self.pos += 1
        return tok

    def eat_if(self, expected_type=None, expected_value=None):

        tok = self.peek()

        if expected_type and tok.type != expected_type:
            return None

        if expected_value and str(tok.value).upper() != expected_value:
            return None

        self.pos += 1
        return tok

    # =============================================================
    # MAIN PARSER DISPATCH
    # =============================================================

    def parse(self):

        while self.peek().type != TokenType.KEYWORD:
            self.pos += 1

        keyword = str(self.peek().value).upper()

        if keyword == "DATA":
            return self.parse_data_step()

        if keyword == "PROC":

            self.eat(TokenType.KEYWORD, "PROC")

            # PROC name can be KEYWORD or IDENTIFIER depending on tokenizer
            tok = self.eat()
            proc_name = str(tok.value).upper()

            if proc_name == "SORT":
                return self.parse_proc_sort()

            if proc_name == "MEANS":
                return self.parse_proc_means()

            if proc_name == "SQL":
                return self.parse_proc_sql()

            raise ParseError(f"Unsupported PROC type: {proc_name}")

        raise ParseError(f"Unsupported block starting with {keyword}")

    # =============================================================
    # DATA STEP PARSER
    # =============================================================

    def parse_data_step(self):

        self.eat(TokenType.KEYWORD, "DATA")

        output_ds = self.eat(TokenType.IDENTIFIER).value

        self.eat(TokenType.PUNCT, ";")

        self.eat(TokenType.KEYWORD, "SET")

        input_ds = self.eat(TokenType.IDENTIFIER).value

        self.eat(TokenType.PUNCT, ";")

        node = DataStepNode(output_ds, input_ds)

        while str(self.peek().value).upper() != "RUN":

            tok = self.peek()

            if str(tok.value).upper() == "WHERE":

                self.eat()

                expr = self.parse_expression()

                node.where.append(expr)

                self.eat(TokenType.PUNCT, ";")

            elif str(tok.value).upper() == "IF":

                self.eat()

                condition = self.parse_expression()

                self.eat(TokenType.KEYWORD, "THEN")

                lhs = self.eat(TokenType.IDENTIFIER).value

                self.eat(TokenType.OPERATOR, "=")

                rhs = self.parse_expression()

                self.eat(TokenType.PUNCT, ";")

                then_assign = AssignNode(lhs, rhs)

                else_assign = None

                if str(self.peek().value).upper() == "ELSE":

                    self.eat()

                    lhs2 = self.eat(TokenType.IDENTIFIER).value

                    self.eat(TokenType.OPERATOR, "=")

                    rhs2 = self.parse_expression()

                    self.eat(TokenType.PUNCT, ";")

                    else_assign = AssignNode(lhs2, rhs2)

                node.if_then.append(
                    IfThenNode(condition, then_assign, else_assign)
                )

            elif tok.type == TokenType.IDENTIFIER:

                lhs = self.eat().value

                self.eat(TokenType.OPERATOR, "=")

                rhs = self.parse_expression()

                self.eat(TokenType.PUNCT, ";")

                node.assignments.append(
                    AssignNode(lhs, rhs)
                )

            else:
                self.pos += 1

        self.eat(TokenType.KEYWORD, "RUN")
        self.eat_if(TokenType.PUNCT, ";")

        return node

    # =============================================================
    # PROC SORT
    # =============================================================

    def parse_proc_sort(self):

        input_ds = None
        output_ds = None
        by_vars = []

        while str(self.peek().value).upper() != "BY":

            tok = self.peek()

            if str(tok.value).upper() == "DATA":

                self.eat()

                self.eat(TokenType.OPERATOR, "=")

                input_ds = self.eat(TokenType.IDENTIFIER).value

            elif str(tok.value).upper() == "OUT":

                self.eat()

                self.eat(TokenType.OPERATOR, "=")

                output_ds = self.eat(TokenType.IDENTIFIER).value

            else:
                self.pos += 1

        self.eat(TokenType.KEYWORD, "BY")

        while self.peek().type != TokenType.PUNCT:

            desc = False

            if str(self.peek().value).upper() == "DESCENDING":
                self.eat()
                desc = True

            col = self.eat(TokenType.IDENTIFIER).value

            by_vars.append(
                {
                    "col": col,
                    "desc": desc
                }
            )

        self.eat(TokenType.PUNCT, ";")

        while str(self.peek().value).upper() != "RUN":
            self.pos += 1

        self.eat(TokenType.KEYWORD, "RUN")
        self.eat_if(TokenType.PUNCT, ";")

        return ProcSortNode(input_ds, output_ds, by_vars)

    # =============================================================
    # PROC MEANS
    # =============================================================

    def parse_proc_means(self):

        input_ds = None
        class_vars = []
        stat_vars = []
        stats = []

        while str(self.peek().value).upper() != "RUN":

            tok = self.peek()
            val = str(tok.value).upper()

            if val == "DATA":

                self.eat()
                self.eat(TokenType.OPERATOR, "=")

                input_ds = self.eat(TokenType.IDENTIFIER).value

            elif val == "CLASS":

                self.eat()

                while self.peek().type != TokenType.PUNCT:
                    class_vars.append(self.eat(TokenType.IDENTIFIER).value)

                self.eat(TokenType.PUNCT, ";")

            elif val == "VAR":

                self.eat()

                while self.peek().type != TokenType.PUNCT:
                    stat_vars.append(self.eat(TokenType.IDENTIFIER).value)

                self.eat(TokenType.PUNCT, ";")

            elif val == "OUTPUT":

                self.eat()   # consume OUTPUT

                # --------------------------------
                # OUT=dataset
                # --------------------------------
                if str(self.peek().value).upper() == "OUT":

                    self.eat()                     # OUT
                    self.eat(TokenType.OPERATOR,"=")

                    # dataset name (identifier or keyword)
                    self.eat()

                # --------------------------------
                # parse statistics
                # --------------------------------
                while self.peek().type != TokenType.PUNCT:

                    tok = self.peek()
                    stat_token = str(tok.value).upper()

                    if stat_token in {
                        "MEAN","SUM","MAX","MIN",
                        "STD","STDDEV","N"
                    }:

                        stats.append(stat_token)
                        self.eat()

                        if self.eat_if(TokenType.OPERATOR,"="):

                            # skip alias names
                            while (
                                self.peek().type not in (TokenType.KEYWORD,TokenType.PUNCT)
                            ):
                                self.pos += 1

                    else:
                        # IMPORTANT: always advance pointer
                        self.pos += 1

                self.eat(TokenType.PUNCT,";")
        return ProcMeansNode(
            input_ds=input_ds,
            class_vars=class_vars,
            stat_vars=stat_vars,
            stats=stats
        )

    # =============================================================
    # PROC SQL → AST
    # =============================================================

    def parse_proc_sql(self):

        tokens = []

        while str(self.peek().value).upper() != "QUIT":
            tokens.append(self.eat().value)

        self.eat(TokenType.KEYWORD, "QUIT")
        self.eat_if(TokenType.PUNCT, ";")

        sql = " ".join(str(t) for t in tokens)

        # SELECT
        select_match = re.search(r"SELECT\s+(.*?)\s+FROM", sql, re.IGNORECASE)
        columns = [c.strip() for c in select_match.group(1).split(",")]

        # FROM
        from_match = re.search(r"FROM\s+(\w+)\s*(\w+)?", sql, re.IGNORECASE)

        table = from_match.group(1)
        alias = from_match.group(2)

        from_node = SqlFromNode(table, alias)

        # JOIN
        join_match = re.search(r"JOIN\s+(\w+)\s*(\w+)?", sql, re.IGNORECASE)
        on_match = re.search(r"ON\s+(.*?)\s+(WHERE|GROUP|ORDER|$)", sql, re.IGNORECASE)

        join_node = None

        if join_match and on_match:

            right_table = join_match.group(1)
            right_alias = join_match.group(2)

            left_col, right_col = [x.strip() for x in on_match.group(1).split("=")]

            join_node = SqlJoinNode(
                table=right_table,
                alias=right_alias,
                left_col=left_col,
                right_col=right_col,
            )

        # WHERE
        where_match = re.search(r"WHERE\s+(.*)", sql, re.IGNORECASE)

        where_node = None

        if where_match:
            where_node = SqlWhereNode(where_match.group(1).strip())

        return ProcSqlNode(
            select=SqlSelectNode(columns),
            from_node=from_node,
            join=join_node,
            where=where_node,
        )

    # =============================================================
    # EXPRESSION PARSER
    # =============================================================

    def parse_expression(self):
        return self.parse_or()

    def parse_or(self):

        node = self.parse_and()

        while self.peek().value == "OR":

            op = self.eat().value
            right = self.parse_and()

            node = BinaryOpNode(node, op, right)

        return node

    def parse_and(self):

        node = self.parse_equality()

        while self.peek().value == "AND":

            op = self.eat().value
            right = self.parse_equality()

            node = BinaryOpNode(node, op, right)

        return node

    def parse_equality(self):

        node = self.parse_comparison()

        while self.peek().value in ("=", "!=", "^=", "NE", "EQ"):

            op = self.eat().value
            right = self.parse_comparison()

            node = BinaryOpNode(node, op, right)

        return node

    def parse_comparison(self):

        node = self.parse_term()

        if self.peek().value == "BETWEEN":

            self.eat()

            low = self.parse_term()

            self.eat(TokenType.KEYWORD, "AND")

            high = self.parse_term()

            return BetweenOpNode(node, low, high)

        if self.peek().value == "IN":

            self.eat()

            self.eat(TokenType.PUNCT, "(")

            values = []

            while True:

                values.append(self.parse_expression())

                if self.eat_if(TokenType.PUNCT, ","):
                    continue

                break

            self.eat(TokenType.PUNCT, ")")

            return InOpNode(node, values)

        while self.peek().value in (">", "<", ">=", "<=", "GT", "LT", "GE", "LE"):

            op = self.eat().value
            right = self.parse_term()

            node = BinaryOpNode(node, op, right)

        return node

    def parse_term(self):

        node = self.parse_factor()

        while self.peek().value in ("+", "-"):

            op = self.eat().value
            right = self.parse_factor()

            node = BinaryOpNode(node, op, right)

        return node

    def parse_factor(self):

        node = self.parse_unary()

        while self.peek().value in ("*", "/"):

            op = self.eat().value
            right = self.parse_unary()

            node = BinaryOpNode(node, op, right)

        return node

    def parse_unary(self):

        if self.peek().value == "NOT":

            op = self.eat().value
            operand = self.parse_unary()

            return UnaryOpNode(op, operand)

        if self.peek().value == "-":

            op = self.eat().value
            operand = self.parse_unary()

            return UnaryOpNode(op, operand)

        return self.parse_primary()

    def parse_primary(self):

        tok = self.peek()

        if tok.type == TokenType.LITERAL_NUM:

            self.eat()

            return LiteralNode(tok.value)

        if tok.type == TokenType.LITERAL_STR:

            self.eat()

            return LiteralNode(tok.value)

        if tok.type == TokenType.IDENTIFIER:

            name = self.eat().value

            if self.eat_if(TokenType.PUNCT, "("):

                args = []

                if not self.eat_if(TokenType.PUNCT, ")"):

                    while True:

                        args.append(self.parse_expression())

                        if self.eat_if(TokenType.PUNCT, ","):
                            continue

                        self.eat(TokenType.PUNCT, ")")
                        break

                return FunctionCallNode(name, args)

            return IdentifierNode(name)

        if self.eat_if(TokenType.PUNCT, "("):

            node = self.parse_expression()

            self.eat(TokenType.PUNCT, ")")

            return node

        raise ParseError(f"Unexpected token {tok}")


# =============================================================
# run_stage3
# =============================================================

def run_stage3(token_map):

    ast_nodes = []

    for block_idx, tokens in token_map.items():

        parser = Parser(tokens)

        try:

            node = parser.parse()

            ast_nodes.append(node)

            log.info(f"Block {block_idx} parsed successfully")

        except Exception as e:

            log.error(f"Block {block_idx} failed: {e}")

            print("\n===== FAILED BLOCK DEBUG =====")
            print(f"Block Index: {block_idx}")
            print("First 15 tokens:")

            for t in tokens[:15]:
                print(f"{t.type.name}  |  {t.value}")

            print("================================\n")

    return ast_nodes