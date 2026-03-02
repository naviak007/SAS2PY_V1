"""
=============================================================
SAS to PySpark Accelerator — Stage 1: Pre-Processor
=============================================================
Takes a raw .sas file and produces a list of clean SAS blocks
ready to be tokenized in Stage 2.

Sub-steps:
  1. Read file  (detect encoding)
  2. Strip comments
  3. Resolve %INCLUDE
  4. Build macro symbol table (%LET)
  5. Substitute macro variables (&var)
  6. Split into logical blocks
=============================================================
"""

import re
import logging
from pathlib import Path
from collections import OrderedDict
from dataclasses import dataclass, field
from typing import List, Optional

# ── Logging setup ─────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("stage1")


# ── Data structures ───────────────────────────────────────────

@dataclass
class SASBlock:
    """One logical SAS block extracted from the source file."""
    block_type: str          # DATA_STEP | PROC_SQL | PROC_SORT | PROC_MEANS | PROC_OTHER | UNKNOWN
    raw_text:   str          # Cleaned SAS text of just this block
    index:      int          # Position in the file (1-based)
    source_file: str = ""


@dataclass
class PreProcessorResult:
    """Everything Stage 1 produces for Stage 2."""
    source_file:   str
    original_text: str
    cleaned_text:  str
    symbol_table:  dict            # resolved macro variables
    blocks:        List[SASBlock]
    warnings:      List[str] = field(default_factory=list)


# ── STEP 1: Read file ─────────────────────────────────────────

def read_file(filepath: str) -> str:
    """Read a .sas file, auto-detecting encoding."""
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"SAS file not found: {filepath}")

    # Try common encodings in order
    for enc in ["utf-8", "latin-1", "cp1252"]:
        try:
            text = path.read_text(encoding=enc)
            log.info(f"  [Step 1] Read {path.name}  ({len(text)} chars, encoding={enc})")
            return text
        except UnicodeDecodeError:
            continue

    raise ValueError(f"Could not decode {filepath} with any supported encoding")


# ── STEP 2: Strip comments ────────────────────────────────────

def strip_comments(source: str) -> str:
    """
    Remove two types of SAS comments:
      a) Block comments:   /* ... */  (can span multiple lines)
      b) Line comments:    * text ;   (standalone statement starting with *)
    """
    # 2a. Block comments  /* ... */
    result = re.sub(r"/\*.*?\*/", "", source, flags=re.DOTALL)

    # 2b. SAS line comments:  lines where the first non-space token is *
    #     Must end with ;  — careful not to strip  *varname  expressions
    result = re.sub(r"(?m)^\s*\*[^;]*;", "", result)

    # Collapse blank lines left behind
    result = re.sub(r"\n{3,}", "\n\n", result)

    log.info(f"  [Step 2] Comments stripped  ({len(source) - len(result)} chars removed)")
    return result


# ── STEP 3: Resolve %INCLUDE ──────────────────────────────────

def resolve_includes(source: str, base_dir: str, warnings: List[str]) -> str:
    """
    Instead of inlining %INCLUDE files, replace each occurrence with a
    placeholder comment that Stage 2+ can convert into a PySpark function call.

    e.g.  %INCLUDE 'hr_utils.sas';
          becomes
          # PYSPARK_CALL: hr_utils()
    """
    pattern = re.compile(r"%INCLUDE\s+['\"](.+?)['\"];", re.IGNORECASE)

    def replacer(match):
        raw_path = match.group(1)                         # e.g. 'utils/hr_utils.sas'
        func_name = Path(raw_path).stem                   # e.g. 'hr_utils'
        placeholder = f"# PYSPARK_CALL: {func_name}()"
        log.info(f"  [Step 3] %INCLUDE '{raw_path}' → placeholder: {placeholder}")
        return placeholder

    result = pattern.sub(replacer, source)

    includes_found = len(pattern.findall(source))
    log.info(f"  [Step 3] %INCLUDE statements converted to placeholders: {includes_found}")
    return result


# ── STEP 4: Build macro symbol table ─────────────────────────

def build_symbol_table(source: str) -> OrderedDict:
    """
    Find all  %LET varname = value;  definitions and store them
    in an OrderedDict so later %LETs can reference earlier ones.
    """
    symbol_table = OrderedDict()

    # Pattern: %LET  varname  =  value  ;
    pattern = re.compile(
        r"%LET\s+(\w+)\s*=\s*([^;]+);",
        re.IGNORECASE
    )

    for match in pattern.finditer(source):
        name  = match.group(1).strip()
        value = match.group(2).strip()

        # A value may itself reference an earlier macro variable
        for k, v in symbol_table.items():
            value = re.sub(rf"&{k}\.?", v, value, flags=re.IGNORECASE)

        symbol_table[name] = value
        log.info(f"  [Step 4] Symbol table: &{name} = '{value}'")

    log.info(f"  [Step 4] Total macro variables found: {len(symbol_table)}")
    return symbol_table


# ── STEP 5: Substitute macro variables ───────────────────────

def substitute_macros(source: str, symbol_table: OrderedDict) -> str:
    """
    Replace every &varname  and  &varname.  occurrence in the source
    with its resolved value from the symbol table.
    Iterates until no more substitutions are possible.
    """
    MAX_PASSES = 10
    result = source

    for pass_num in range(MAX_PASSES):
        changed = False
        for name, value in symbol_table.items():
            # Match &varname. (with trailing dot) or &varname (without)
            new_result = re.sub(
                rf"&{name}\.?",
                value,
                result,
                flags=re.IGNORECASE
            )
            if new_result != result:
                changed = True
                result = new_result

        if not changed:
            log.info(f"  [Step 5] Macro substitution complete after {pass_num + 1} pass(es)")
            break

    # Warn about any remaining & references (unresolved macros)
    unresolved = re.findall(r"&\w+\.?", result)
    if unresolved:
        log.warning(f"  [Step 5] Unresolved macro references: {set(unresolved)}")

    return result


# ── STEP 6: Split into logical blocks ────────────────────────

def split_into_blocks(source: str, source_file: str) -> List[SASBlock]:
    """
    Split the cleaned SAS source into individual logical blocks.
    Each block is one of:
      - DATA step      (DATA ... RUN;)
      - PROC SQL       (PROC SQL ... QUIT;)
      - PROC SORT      (PROC SORT ... RUN;)
      - PROC MEANS     (PROC MEANS ... RUN;)
      - PROC OTHER     (any other PROC ... RUN;)
    """

    # Pattern captures everything from DATA/PROC keyword to RUN; or QUIT;
    pattern = re.compile(
        r"(DATA\b.+?RUN\s*;|PROC\s+SQL\b.+?QUIT\s*;|PROC\s+\w+\b.+?RUN\s*;)",
        re.IGNORECASE | re.DOTALL
    )

    blocks = []
    for idx, match in enumerate(pattern.finditer(source), start=1):
        raw_text = match.group(1).strip()

        # Classify the block type
        upper = raw_text.upper().lstrip()
        if upper.startswith("DATA"):
            block_type = "DATA_STEP"
        elif re.match(r"PROC\s+SQL", upper):
            block_type = "PROC_SQL"
        elif re.match(r"PROC\s+SORT", upper):
            block_type = "PROC_SORT"
        elif re.match(r"PROC\s+MEANS", upper):
            block_type = "PROC_MEANS"
        else:
            block_type = "PROC_OTHER"

        block = SASBlock(
            block_type=block_type,
            raw_text=raw_text,
            index=idx,
            source_file=source_file
        )
        blocks.append(block)
        log.info(f"  [Step 6] Block {idx}: {block_type}")

    log.info(f"  [Step 6] Total blocks extracted: {len(blocks)}")
    return blocks



# ── Save result to file ───────────────────────────────────────

def save_result_to_file(result: PreProcessorResult, output_path: str):
    """
    Save Stage 1 result to a text file.
    If file exists → overwrite.
    If not → create new file.
    """

    output_file = Path(output_path)

    lines = []
    SEP = "─" * 55

    lines.append(f"{'═'*55}")
    lines.append(f"STAGE 1 RESULTS — {Path(result.source_file).name}")
    lines.append(f"{'═'*55}\n")

    # Macro table
    lines.append(f"MACRO SYMBOL TABLE ({len(result.symbol_table)} variables)")
    lines.append(SEP)
    for name, value in result.symbol_table.items():
        lines.append(f"&{name:<20} = '{value}'")

    lines.append("\n")

    # Blocks
    lines.append(f"EXTRACTED BLOCKS ({len(result.blocks)} total)")
    lines.append(SEP)

    for block in result.blocks:
        lines.append(f"\n[{block.index}] {block.block_type}")
        lines.append("─" * 50)
        lines.append(block.raw_text)
        lines.append("\n")

    # Warnings
    if result.warnings:
        lines.append(f"\nWARNINGS ({len(result.warnings)})")
        lines.append(SEP)
        for w in result.warnings:
            lines.append(f"• {w}")

    # Write file (overwrite mode)
    output_file.write_text("\n".join(lines), encoding="utf-8")

    log.info(f"[OUTPUT] Stage 1 result saved to: {output_file}")

# ── MAIN: Pre-Processor orchestrator ─────────────────────────

def run_stage1(sas_filepath: str) -> PreProcessorResult:
    """
    Run all 6 sub-steps of Stage 1 and return a PreProcessorResult
    containing the symbol table, cleaned text, and block list.
    """
    warnings = []
    path     = Path(sas_filepath)
    base_dir = str(path.parent)

    log.info("=" * 55)
    log.info(f"STAGE 1 — Pre-Processor: {path.name}")
    log.info("=" * 55)

    # Sub-step 1: Read
    original = read_file(sas_filepath)

    # Sub-step 2: Strip comments
    cleaned = strip_comments(original)

    # Sub-step 3: Resolve %INCLUDE
    cleaned = resolve_includes(cleaned, base_dir, warnings)

    # Sub-step 4: Build symbol table
    symbol_table = build_symbol_table(cleaned)

    # Sub-step 5: Substitute macro variables
    cleaned = substitute_macros(cleaned, symbol_table)

    # Sub-step 6: Split into blocks
    blocks = split_into_blocks(cleaned, path.name)

    log.info("=" * 55)
    log.info(f"Stage 1 complete  |  {len(blocks)} blocks  |  {len(warnings)} warnings")
    log.info("=" * 55)

    output_filename = path.stem + "_stage1_output.txt"
    save_result_to_file(
        PreProcessorResult(
            source_file   = str(path),
            original_text = original,
            cleaned_text  = cleaned,
            symbol_table  = dict(symbol_table),
            blocks        = blocks,
            warnings      = warnings,
        ),
        output_filename
    )
    
    return PreProcessorResult(
        source_file   = str(path),
        original_text = original,
        cleaned_text  = cleaned,
        symbol_table  = dict(symbol_table),
        blocks        = blocks,
        warnings      = warnings,
    )


# ── Pretty printer for review ─────────────────────────────────

def print_result(result: PreProcessorResult):
    SEP = "─" * 55

    print(f"\n{'═'*55}")
    print(f"  STAGE 1 RESULTS — {Path(result.source_file).name}")
    print(f"{'═'*55}")

    print(f"\n MACRO SYMBOL TABLE ({len(result.symbol_table)} variables)")
    print(SEP)
    for name, value in result.symbol_table.items():
        print(f"  &{name:<20} = '{value}'")

    print(f"\n EXTRACTED BLOCKS ({len(result.blocks)} total)")
    print(SEP)
    for block in result.blocks:
        print(f"\n  [{block.index}] {block.block_type}")
        print(f"  {'─'*50}")
        for line in block.raw_text.splitlines():
            if line.strip():
                print(f"    {line}")

    if result.warnings:
        print(f"\n  WARNINGS ({len(result.warnings)})")
        print(SEP)
        for w in result.warnings:
            print(f"  • {w}")

    print(f"\n{'═'*55}\n")


"""if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("Usage: python stage1_preprocessor.py <sas_file>")
        sys.exit(1)

    sas_file = sys.argv[1]
    result = run_stage1(sas_file)
    """