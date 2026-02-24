# SAS2PY_V1
---
# SAS to PySpark Accelerator

A structured, multi-stage compiler pipeline that converts legacy SAS programs into production-ready PySpark code.

---

## Overview

The SAS to PySpark Accelerator is a modular transformation engine designed to migrate SAS workloads to PySpark using a compiler-style architecture.

The system performs deterministic, rule-based translation through a clearly defined pipeline consisting of lexical analysis, parsing, abstract syntax tree generation, code translation, formatting, and validation.

This architecture ensures:

* Maintainability
* Extensibility
* Clear stage separation
* Production-grade output formatting
* Transparent handling of unsupported constructs

---

## Architecture

The accelerator follows a six-stage pipeline:

```
SAS Source
   ↓
Stage 1 — Preprocessor
   ↓
Stage 2 — Tokenizer (Lexer)
   ↓
Stage 3 — Parser (AST Builder)
   ↓
Stage 4 — Translator (SAS → PySpark)
   ↓
Stage 5 — Post-Processor (Formatting + Metadata)
   ↓
Stage 6 — Validator (Syntax + TODO checks)
```

Each stage has a single responsibility and produces structured output for the next stage.

---

## Stage Breakdown

### Stage 1 — Preprocessor

* Reads SAS files with encoding detection
* Removes block and line comments
* Resolves `%INCLUDE` statements
* Builds macro symbol table (`%LET`)
* Substitutes macro variables (`&var`)
* Splits source into logical SAS blocks

Output: Cleaned SAS blocks and metadata.

---

### Stage 2 — Tokenizer (Lexer)

* Converts cleaned SAS blocks into typed tokens
* Classifies:

  * Keywords
  * Identifiers
  * Numeric literals
  * String literals
  * Operators
  * Punctuation
* Tracks line numbers
* Appends EOF sentinel

Output: Token map per block.

---

### Stage 3 — Parser

* Implements recursive-descent parsing
* Converts token streams into Abstract Syntax Tree (AST) nodes
* Supports:

  * DATA steps
  * PROC SORT
  * PROC MEANS / SUMMARY
  * PROC SQL

Output: List of structured AST nodes.

---

### Stage 4 — Translator

* Traverses AST nodes
* Converts SAS constructs into PySpark DataFrame API
* Maps SAS built-in functions to PySpark equivalents
* Preserves SQL logic using `spark.sql()`
* Marks unsupported constructs with TODO comments

Output: Raw PySpark code string and TODO count.

---

### Stage 5 — Post-Processor

* Builds structured file header with metadata
* Deduplicates and sorts imports
* Injects SparkSession initialization if required
* Formats code using `black`
* Generates metadata JSON
* Writes final `.py` file to disk

Output: Production-ready PySpark script.

---

### Stage 6 — Validator

* Parses generated Python code to validate syntax
* Scans for TODO markers
* Generates validation JSON report
* Outputs PASS / WARN / FAIL summary

Output: Validation report and console summary.

---

## Installation

### Clone the Repository

```
git clone https://github.com/<your-username>/sas-to-pyspark-accelerator.git
cd sas-to-pyspark-accelerator
```

### Install Dependencies

```
pip install -r requirements.txt
```

### Requirements

```
black
isort
pyspark
```

---

## Usage

Run the full pipeline:

```
python stage6_validator.py input_file.sas
```

---

## Output

For an input file:

```
input_file.sas
```

The accelerator generates:

```
input_file_Converted.py
input_file_Converted_stage6_report.json
```

Both files are written to the same directory as the source file.

---

## Example

### Input (SAS)

```
DATA output;
    SET input;
    WHERE age > 30;
RUN;
```

### Output (PySpark)

```
df_output = df_input \
    .filter("age > 30")
```

---

## Validation Report Example

```
{
  "file": "input_file_Converted.py",
  "status": "PASS",
  "timestamp": "...",
  "checks": [
    {
      "name": "Python syntax",
      "status": "PASS",
      "detail": "No syntax errors"
    }
  ]
}
```

---

## Design Principles

* Modular, stage-based architecture
* Deterministic transformation
* No hardcoded paths
* CLI-driven execution
* Clear separation of concerns
* Explicit TODO markers for unsupported logic
* PEP 8 compliant output formatting

---

## Limitations

* Not all SAS procedures are currently supported
* Highly complex macro logic may require manual refactoring
* Some advanced PROC options may require enhancement

Unsupported constructs are clearly marked in the output:

```
# TODO: manual conversion required
```

---

## Extensibility

The system is designed for extension.

To support new SAS procedures:

1. Extend parsing logic in Stage 3
2. Implement translation logic in Stage 4
3. Register translator in dispatch layer
4. Add validation rules in Stage 6 if required

The architecture supports incremental enhancement without impacting other stages.

---

## Technical Highlights

* Recursive-descent parser implementation
* Abstract Syntax Tree (AST) based translation
* Structured token classification
* Import normalization and formatting automation
* Automated SparkSession injection
* Validation reporting framework
* End-to-end CLI pipeline

---

## License

Specify appropriate license (MIT, internal enterprise use, etc.).

---

## Author

Navia Narayanan
Data Engineering and Platform Modernization

---

If you would like, I can now:

* Refine this for a GitHub portfolio presentation
* Rewrite it in a more corporate enterprise tone
* Convert it into a one-page architecture document
* Add a “Why this matters” business justification section
* Create a professional LinkedIn project description based on this

Tell me where you plan to use it.
