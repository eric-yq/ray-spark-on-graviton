"""Standard TPC-H SQL (subset) plus one extra big two-table join.

Substitution parameters use the TPC-H validation defaults. These exercise the
representative SparkSQL patterns we want to compare across architectures:

  q1          low-cardinality group + many aggregates (scan + light shuffle)
  q6          selective filter + single aggregate (scan-bound)
  q5          6-table star join + group (join-heavy)
  q9          6-table join + derived arithmetic + group (heaviest)
  join_orders lineitem ⨝ orders, group by priority (large hash-join shuffle)
"""

QUERIES = {
    "q1": """
        SELECT l_returnflag, l_linestatus,
               sum(l_quantity)                                   AS sum_qty,
               sum(l_extendedprice)                              AS sum_base_price,
               sum(l_extendedprice * (1 - l_discount))           AS sum_disc_price,
               sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
               avg(l_quantity)                                   AS avg_qty,
               avg(l_extendedprice)                              AS avg_price,
               avg(l_discount)                                   AS avg_disc,
               count(*)                                          AS count_order
        FROM lineitem
        WHERE l_shipdate <= date '1998-09-02'
        GROUP BY l_returnflag, l_linestatus
        ORDER BY l_returnflag, l_linestatus
    """,
    "q6": """
        SELECT sum(l_extendedprice * l_discount) AS revenue
        FROM lineitem
        WHERE l_shipdate >= date '1994-01-01'
          AND l_shipdate <  date '1995-01-01'
          AND l_discount BETWEEN 0.05 AND 0.07
          AND l_quantity < 24
    """,
    "q5": """
        SELECT n_name, sum(l_extendedprice * (1 - l_discount)) AS revenue
        FROM customer, orders, lineitem, supplier, nation, region
        WHERE c_custkey = o_custkey
          AND l_orderkey = o_orderkey
          AND l_suppkey = s_suppkey
          AND c_nationkey = s_nationkey
          AND s_nationkey = n_nationkey
          AND n_regionkey = r_regionkey
          AND r_name = 'ASIA'
          AND o_orderdate >= date '1994-01-01'
          AND o_orderdate <  date '1995-01-01'
        GROUP BY n_name
        ORDER BY revenue DESC
    """,
    "q9": """
        SELECT nation, o_year, sum(amount) AS sum_profit
        FROM (
            SELECT n_name AS nation,
                   year(o_orderdate) AS o_year,
                   l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity AS amount
            FROM part, supplier, lineitem, partsupp, orders, nation
            WHERE s_suppkey = l_suppkey
              AND ps_suppkey = l_suppkey
              AND ps_partkey = l_partkey
              AND p_partkey = l_partkey
              AND o_orderkey = l_orderkey
              AND s_nationkey = n_nationkey
              AND p_name LIKE '%green%'
        ) AS profit
        GROUP BY nation, o_year
        ORDER BY nation, o_year DESC
    """,
    "join_orders": """
        SELECT o_orderpriority,
               count(*)                                AS line_count,
               sum(l_extendedprice * (1 - l_discount)) AS revenue
        FROM orders, lineitem
        WHERE l_orderkey = o_orderkey
        GROUP BY o_orderpriority
        ORDER BY o_orderpriority
    """,
}
