# PostgreSQL Query Optimization: Lessons from Production

Efficient query execution is critical for maintaining the performance and responsiveness of database applications. This guide covers key strategies for query optimization, focusing on the use of B-tree and GIN indexes, and walks through case studies showing the impact of these optimizations on real queries.

The goal is to serve as a shared reference for the team on query optimization — contributions, corrections, and additional tips are welcome.

---

## B-tree Indexes

**B-tree indexes** are the default and most commonly used index type in PostgreSQL. They are effective for:

- Exact match queries.
- Range queries (e.g., `BETWEEN`, `<`, `>`).
- Queries involving sorted order.

**Example:**

```sql
CREATE INDEX idx_account_number ON invoicedetails (account_number);
```

**When to use:**

- Columns with high cardinality (many unique values).
- Columns frequently used in `WHERE`, `ORDER BY`, and `GROUP BY` clauses.

## GIN Indexes

**GIN (Generalized Inverted Index)** indexes handle more complex queries involving full-text search, arrays, and JSONB data types. They're particularly useful for:

- Full-text search.
- Similarity searches with the `pg_trgm` module.
- Fast lookup of multiple values (array containment/overlap).

**Example:**

```sql
CREATE INDEX idx_tags_gin ON documents USING GIN (tags);
```

**When to use:**

- Columns containing arrays, JSONB, or text requiring full-text search.
- Queries involving `LIKE '%pattern%'` (combined with `pg_trgm`, see below).

## Composite Indexes

**Composite indexes** (multicolumn indexes) index multiple columns together. They're effective for queries that filter or sort on multiple columns simultaneously.

**Example:**

```sql
CREATE INDEX idx_account_transaction_date
  ON invoicedetails (account_number, transaction_date);
```

**When to use:**

- Queries involving multiple columns in `WHERE`, `ORDER BY`, or `GROUP BY` clauses.
- When individual columns don't provide sufficient selectivity but their combination does.

---

## Impact of Functions and Wildcards on Query Performance

### `LOWER()` Function

Using `LOWER()` in `WHERE` clauses can significantly impact performance, because Postgres must apply the function to every row, which prevents the planner from using a plain index on the underlying column.

**Example:**

```sql
SELECT * FROM invoicedetails WHERE LOWER(product) = 'product1';
```

**Impact:**

- Forces a full table (or full index) scan if there's no index on the expression `LOWER(product)`.
- A plain index on `product` cannot be used for this predicate.

### `TRIM()` Function

Similarly, `TRIM()` can degrade performance by preventing index use, since it requires evaluating the expression for every row before comparison.

**Example:**

```sql
SELECT * FROM invoicedetails
WHERE TRIM(BOTH ' ' FROM sub_customer_name) = '';
```

**Impact:**

- Prevents use of a plain index on `sub_customer_name`.
- Forces a full scan unless a matching functional index exists.

### Wildcards (`LIKE '%pattern%'`)

Leading wildcards in `LIKE` patterns also cause performance issues, since Postgres must scan the entire column to find matches — a plain B-tree index can only help with patterns that don't start with a wildcard (e.g., `'prefix%'`).

**Example:**

```sql
SELECT * FROM invoicedetails
WHERE LOWER(sub_customer_name) LIKE '%customer%';
```

**Impact:**

- A leading wildcard prevents the use of standard B-tree indexes.
- Forces a full scan unless a GIN index with `gin_trgm_ops` is used.

---

## Case Study 1: Wildcard / Functional Predicates

Consider the following query, which initially took 16 seconds to execute on a large dataset:

```sql
SELECT *
FROM invoicedetails invoicedet0_
WHERE
    (invoicedet0_.account_number IN ('123456', '789012'))
    AND (invoicedet0_.transaction_id IN ('tx1001', 'tx1002', 'tx1003'))
    AND (
        invoicedet0_.sales_order = 'SO12345'
        OR invoicedet0_.billing_agreement = 'BA67890'
        OR invoicedet0_.purchase_order = 'PO111213'
    )
    AND (LOWER(invoicedet0_.product) IN ('product1'))
    AND (LOWER(invoicedet0_.periodic_charges) IN ('monthly'))
    AND (LOWER(invoicedet0_.ibx) IN ('nyc1'))
    AND invoicedet0_.transaction_date >= '2023-01-01'
    AND invoicedet0_.transaction_date <= '2023-12-31'
    AND (
        LOWER(invoicedet0_.sub_customer_name) LIKE '%customer%'
        OR invoicedet0_.sub_customer_name IS NULL
        OR TRIM(BOTH ' ' FROM invoicedet0_.sub_customer_name) = ''
        OR LOWER(invoicedet0_.reseller_name) LIKE '%reseller%'
    )
    AND (
        LOWER(invoicedet0_.sub_customer_account) LIKE '%account%'
        OR invoicedet0_.sub_customer_account IS NULL
        OR TRIM(BOTH ' ' FROM invoicedet0_.sub_customer_account) = ''
        OR LOWER(invoicedet0_.reseller_account) LIKE '%reseller_acc%'
    )
ORDER BY
    invoicedet0_.detail_line_number ASC,
    invoicedet0_.sub_line_number ASC
LIMIT 100;
```

> **Note on the original draft:** the source query had a bare `OR` joining the trailing wildcard/null-check block to the rest of the `AND` chain, with no enclosing parentheses. Because `AND` binds tighter than `OR` in SQL, that meant *any* row matching just the `sub_customer_name`/`reseller_name` clause would be returned, even if it failed every other filter (account number, transaction ID, date range, etc.). That's a correctness bug, not just a performance one. The version above wraps each "wildcard or null-or-blank" group in parentheses and joins it with `AND` to the rest of the filter, which is almost certainly the intended logic. If the OR-across-everything behavior was actually intentional, call it out explicitly in the query/comments so the next person doesn't "fix" it by accident.

**Challenges faced**

Despite indexes on `account_number` and `transaction_id`, the query remained slow due to:

- Use of `LIKE` and `LOWER()` on multiple columns.
- No index could be used for the wildcard pattern-matching predicates.

**Solution: `pg_trgm` with GIN indexes**

GIN indexes built with the `pg_trgm` operator class are tailored for pattern matching and similarity search, which dominate this query's predicates.

**Steps:**

1. Ensure the `pg_trgm` extension is available:

```sql
SELECT * FROM pg_available_extensions WHERE name = 'pg_trgm';

-- Create the extension if it isn't already installed
CREATE EXTENSION IF NOT EXISTS pg_trgm WITH SCHEMA q2c;
```

2. Create GIN indexes with `gin_trgm_ops` on the lowercased expressions actually used in the queries:

```sql
CREATE INDEX sub_customer_name_lower_trgm_idx
  ON q2c.invoicedetails USING GIN (lower(sub_customer_name) gin_trgm_ops);

CREATE INDEX reseller_name_lower_trgm_idx
  ON q2c.invoicedetails USING GIN (lower(reseller_name) gin_trgm_ops);

CREATE INDEX sub_customer_account_lower_trgm_idx
  ON q2c.invoicedetails USING GIN (lower(sub_customer_account) gin_trgm_ops);

CREATE INDEX reseller_account_lower_trgm_idx
  ON q2c.invoicedetails USING GIN (lower(reseller_account) gin_trgm_ops);
```

3. Run the optimized query with parameters that actually return data, so the plan reflects a real lookup rather than a zero-row short-circuit:

```sql
EXPLAIN ANALYZE
SELECT *
FROM q2c.invoicedetails invoicedet0_
WHERE
    (invoicedet0_.account_number IN ('3890'))
    AND (invoicedet0_.transaction_id IN ('100210594618'))
    AND (
        invoicedet0_.sales_order = '1-221964231536'
        OR invoicedet0_.billing_agreement = '1-196320556821'
        OR invoicedet0_.purchase_order = '1-221367457710'
    )
    AND (
        LOWER(invoicedet0_.sub_customer_name) LIKE '%zayo group%'
        OR invoicedet0_.sub_customer_name IS NULL
        OR TRIM(BOTH ' ' FROM invoicedet0_.sub_customer_name) = ''
        OR LOWER(invoicedet0_.reseller_name) LIKE '%zayo group%'
    )
ORDER BY
    invoicedet0_.detail_line_number ASC,
    invoicedet0_.sub_line_number ASC
LIMIT 100;
```

**Outcome:**

- The query execution time dropped from roughly 16 seconds to tens of milliseconds once the trigram GIN indexes were in place and the planner could use `BitmapOr` across the indexed expressions instead of scanning every row.
- When validating this kind of change yourself, confirm the `EXPLAIN ANALYZE` run actually returns the rows you expect (`rows > 0` somewhere in the plan) — a plan that resolves to zero matching rows can look fast for the wrong reason and doesn't prove the index helps on a real hit.

---

## Case Study 2: Arrays, JSONB, and Joins

Consider the following query:

```sql
SELECT DISTINCT
    invoicesum0_.invoice_summary_id   AS invoice_1_2_,
    invoicesum0_.account_number       AS account_2_2_,
    invoicesum0_.dih_created_at       AS dih_crea3_2_,
    invoicesum0_.dih_created_by       AS dih_crea4_2_,
    invoicesum0_.dih_updated_at       AS dih_upda5_2_,
    invoicesum0_.dih_updated_by       AS dih_upda6_2_,
    invoicesum0_.document_types       AS document7_2_,
    invoicesum0_.file_types           AS file_typ8_2_,
    invoicesum0_.ibx                  AS ibx9_2_,
    invoicesum0_.sub_customer_number  AS sub_cus10_2_,
    invoicesum0_.invoice_summary_info AS invoice11_2_,
    invoicesum0_.transaction_date     AS transac12_2_,
    invoicesum0_.transaction_id       AS transac13_2_,
    invoicesum0_.transaction_languages AS transac14_2_,
    invoicesum0_.transaction_type      AS transac15_2_
FROM invoicesummary invoicesum0_
LEFT OUTER JOIN creditmemo invoicecre1_
    ON invoicesum0_.invoice_summary_id = invoicecre1_.invoice_summary_id
WHERE
    invoicesum0_.account_number IN ('658349')
    AND invoicesum0_.transaction_id IN ('182220000020')
    AND invoicesum0_.sub_customer_number && ARRAY['764853', '8547685']
    AND invoicesum0_.ibx && ARRAY['CD2', 'FR2']
    AND invoicesum0_.document_types && ARRAY['EQUINIX', 'GOVERNMENT']
    AND jsonb_extract_path_text(invoicesum0_.invoice_summary_info, 'equinixRegion')
        IN ('AMER', 'EMEA')
    AND invoicesum0_.transaction_date >= '2024-01-25'
    AND invoicesum0_.transaction_date <= '2024-01-30'
ORDER BY invoicesum0_.transaction_date ASC
LIMIT 25;
```

> **Note on the original draft:** the source used `array_overlaps(col, ARRAY[...]) = true`, which is not a built-in PostgreSQL function — it likely came from an ORM helper or a project-specific wrapper. The native equivalent, and what the GIN index on an array column actually accelerates, is the overlap operator `&&` (e.g., `col && ARRAY[...]`). If your codebase really does define `array_overlaps()` as a SQL function or operator wrapper, keep it — but call that out explicitly so readers with a plain PostgreSQL install don't hit "function does not exist."

**Optimizing with indexes**

```sql
-- B-tree index on transaction_date
CREATE INDEX idx_invoicesummary_transaction_date
  ON invoicesummary (transaction_date);

-- B-tree index on account_number
CREATE INDEX idx_invoicesummary_account_number
  ON invoicesummary (account_number);

-- B-tree index on transaction_id
CREATE INDEX idx_invoicesummary_transaction_id
  ON invoicesummary (transaction_id);

-- B-tree indexes on the join key, on both sides
CREATE INDEX idx_invoicesummary_invoice_summary_id
  ON invoicesummary (invoice_summary_id);
CREATE INDEX idx_creditmemo_invoice_summary_id
  ON creditmemo (invoice_summary_id);

-- GIN indexes for array columns
CREATE INDEX idx_invoicesummary_sub_customer_number
  ON invoicesummary USING GIN (sub_customer_number);
CREATE INDEX idx_invoicesummary_ibx
  ON invoicesummary USING GIN (ibx);
CREATE INDEX idx_invoicesummary_document_types
  ON invoicesummary USING GIN (document_types);
```

**Reasoning**

- **B-tree indexes** on `transaction_date`, `account_number`, and `transaction_id` optimize the range and exact-match predicates.
- **GIN indexes** on `sub_customer_number`, `ibx`, and `document_types` enable efficient evaluation of the `&&` (array overlap) predicates.
- **Indexes on `invoice_summary_id`** (on both tables) support an efficient join between `invoicesummary` and `creditmemo`.

By combining B-tree and GIN indexes appropriately, you can significantly cut execution time and improve overall database efficiency for queries that mix scalar, array, and JSONB predicates.

---

## Case Study 3: One Composite Index vs. Several

```sql
SELECT * FROM q2c.ucmaddress
  WHERE account_number IN ($1) AND bill_to = 'P'
  LIMIT $2 OFFSET $3;

SELECT * FROM q2c.ucmaddress
  WHERE account_number IN ($1) AND main_addr = 'P'
  LIMIT $2 OFFSET $3;

SELECT * FROM q2c.ucmaddress
  WHERE account_number IN ($1) AND ship_to = 'P'
  LIMIT $2 OFFSET $3;

SELECT * FROM q2c.ucmaddress
  WHERE account_number IN ($1) AND sold_to = 'P'
  LIMIT $2 OFFSET $3;
```

> **Note on the original draft:** the source had a typographic curly apostrophe inside one of the string literals (an artifact of copy-pasting from a rich text editor) and used `limit (limit) offset (offset)` as literal placeholder text. The version above uses standard bind-parameter placeholders (`$1`, `$2`, `$3`) — substitute your driver's actual parameter syntax.

### Problem

We need to optimize these four queries for better performance. The core question: create multiple composite indexes (one per query shape), or a single composite index covering all the address-type columns?

### Option 1: Multiple Composite Indexes

```sql
CREATE INDEX ucmaddress_acc_billto_idx    ON q2c.ucmaddress (account_number, bill_to);
CREATE INDEX ucmaddress_acc_mainaddr_idx  ON q2c.ucmaddress (account_number, main_addr);
CREATE INDEX ucmaddress_acc_shipto_idx    ON q2c.ucmaddress (account_number, ship_to);
CREATE INDEX ucmaddress_acc_soldto_idx    ON q2c.ucmaddress (account_number, sold_to);
```

### Option 2: Single Composite Index

```sql
CREATE INDEX ucmaddress_acc_idx_com
  ON q2c.ucmaddress (account_number, bill_to, main_addr, ship_to, sold_to);
```

### Analysis and Reasoning

1. **Index selectivity and match to query shape**
   - Each of the four queries only filters on `account_number` plus *one* of the four flag columns. A multi-column index is only efficient for a leading-column prefix of the predicates it's given — a query filtering on `(account_number, ship_to)` cannot make efficient use of an index ordered `(account_number, bill_to, main_addr, ship_to, sold_to)` for the `ship_to` predicate, because `bill_to` and `main_addr` sit in between and aren't constrained by that query.
   - Four targeted two-column indexes each match a query's predicate exactly.

2. **Query performance**
   - With four targeted indexes, each query goes straight to the relevant index and avoids scanning data ordered by irrelevant columns.
   - The single wide composite index works well only for queries that filter on `account_number` and `bill_to` (the two leading columns) — it provides little to no benefit for the other three queries.

3. **Index maintenance**
   - Four indexes mean more write overhead (each `INSERT`/`UPDATE`/`DELETE` updates four indexes instead of one).
   - One composite index is cheaper to maintain, but only if it actually serves your read patterns — which it doesn't here for three of the four queries.

### Conclusion

**Option 1 (multiple composite indexes)** is the better fit for this workload because each query's predicate matches one index exactly, and the read-side win outweighs the extra write overhead for a read-heavy access pattern. If write volume on this table is very high, or if these four query shapes also commonly appear *together* in a single query, that tradeoff is worth revisiting.

### Recommended Index Creation

```sql
CREATE INDEX ucmaddress_acc_billto_idx    ON q2c.ucmaddress (account_number, bill_to);
CREATE INDEX ucmaddress_acc_mainaddr_idx  ON q2c.ucmaddress (account_number, main_addr);
CREATE INDEX ucmaddress_acc_shipto_idx    ON q2c.ucmaddress (account_number, ship_to);
CREATE INDEX ucmaddress_acc_soldto_idx    ON q2c.ucmaddress (account_number, sold_to);
```

---

## Best Practices for Indexing

1. **Index selection**
   - Use **B-tree indexes** for high-cardinality columns and range queries.
   - Use **GIN indexes** for full-text search, JSONB, and array columns.

2. **Functional indexes**
   - Create indexes on expressions like `LOWER(column)` or `TRIM(column)` to keep queries that use those functions index-friendly.

3. **Data ingestion**
   - Normalize and clean data on ingestion (consistent casing, trimmed whitespace) to reduce the need for `TRIM()`/`LOWER()` in queries entirely, which lets the planner use plain indexes more often.

---

## Sorting by `transaction_id`

We extended the sort for the invoice-details API with an additional parameter, `transaction_id`, so results are ordered by `transaction_id` first, with `detail_line_number` and `sub_line_number` as secondary and tertiary sort keys.

```sql
SELECT
    invoicedet0_.details_id            AS details_1_1_,
    invoicedet0_.account_number        AS account_2_1_,
    invoicedet0_.billing_agreement     AS billing_3_1_,
    invoicedet0_.dih_created_at        AS dih_crea4_1_,
    invoicedet0_.dih_created_by        AS dih_crea5_1_,
    invoicedet0_.dih_updated_at        AS dih_upda6_1_,
    invoicedet0_.dih_updated_by        AS dih_upda7_1_,
    invoicedet0_.ibx                   AS ibx8_1_,
    invoicedet0_.detail_line_number    AS detail_l9_1_,
    invoicedet0_.invoice_details_info  AS invoice10_1_,
    invoicedet0_.periodic_charges      AS periodi11_1_,
    invoicedet0_.product               AS product12_1_,
    invoicedet0_.purchase_order        AS purchas13_1_,
    invoicedet0_.reseller_account      AS reselle14_1_,
    invoicedet0_.reseller_name         AS reselle15_1_,
    invoicedet0_.sales_order           AS sales_o16_1_,
    invoicedet0_.sub_customer_account  AS sub_cus17_1_,
    invoicedet0_.sub_customer_name     AS sub_cus18_1_,
    invoicedet0_.sub_line_number       AS sub_lin19_1_,
    invoicedet0_.transaction_date      AS transac20_1_,
    invoicedet0_.transaction_id        AS transac21_1_
FROM invoicedetails invoicedet0_
WHERE invoicedet0_.account_number IN ('500097')
ORDER BY
    invoicedet0_.transaction_id ASC,
    invoicedet0_.detail_line_number ASC,
    invoicedet0_.sub_line_number ASC
LIMIT 25;
```

To support this sort efficiently, ensure there's a composite index matching the filter + sort order, e.g.:

```sql
CREATE INDEX idx_invoicedetails_account_sort
  ON invoicedetails (account_number, transaction_id, detail_line_number, sub_line_number);
```

This lets Postgres satisfy the `WHERE` filter and produce the `ORDER BY` result without a separate sort step.

---

## Conclusion

Optimizing query execution in PostgreSQL comes down to a few repeatable habits: understand the appropriate use cases for B-tree vs. GIN indexes, minimize functions in predicates that prevent index usage (or build functional indexes when you can't avoid them), clean data at ingestion time, and match index column order to your actual query shapes rather than building one index to cover everything. Verify each change with `EXPLAIN ANALYZE` against representative data — and double check operator precedence in multi-clause `WHERE` predicates, since an unparenthesized `OR` can silently change query *results*, not just query *speed*.
