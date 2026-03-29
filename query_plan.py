#!/usr/bin/env python3
"""Query planner — cost-based optimization for SQL-like queries."""
import sys

class Table:
    def __init__(self, name, rows, cols, indexes=None):
        self.name = name; self.rows = rows; self.cols = cols
        self.indexes = indexes or set()

class ScanOp:
    def __init__(self, table, predicate=None):
        self.table = table; self.predicate = predicate
    def cost(self):
        if self.predicate and self.predicate.get("col") in self.table.indexes:
            return max(1, int(self.table.rows * 0.01))
        return self.table.rows
    def __repr__(self):
        idx = "IndexScan" if self.predicate and self.predicate.get("col") in self.table.indexes else "SeqScan"
        return f"{idx}({self.table.name}, cost={self.cost()})"

class JoinOp:
    def __init__(self, left, right, method="nested"):
        self.left = left; self.right = right; self.method = method
    def cost(self):
        lc, rc = self.left.cost(), self.right.cost()
        if self.method == "nested": return lc * rc
        if self.method == "hash": return lc + rc * 3
        if self.method == "merge": return lc + rc + (lc + rc) * 0.5
        return lc * rc
    def __repr__(self):
        return f"{self.method.title()}Join(cost={self.cost():.0f})"

def optimize_join(left, right):
    best = None; best_cost = float('inf')
    for method in ["nested", "hash", "merge"]:
        j = JoinOp(left, right, method)
        if j.cost() < best_cost:
            best = j; best_cost = j.cost()
    return best

if __name__ == "__main__":
    users = Table("users", 10000, ["id","name","email"], {"id"})
    orders = Table("orders", 50000, ["id","user_id","total"], {"id","user_id"})
    products = Table("products", 1000, ["id","name","price"], {"id"})
    scan_u = ScanOp(users, {"col": "id", "op": "=", "val": 42})
    scan_o = ScanOp(orders)
    scan_p = ScanOp(products, {"col": "id", "op": "=", "val": 1})
    print("Scan costs:")
    for s in [scan_u, scan_o, scan_p]: print(f"  {s}")
    print("\nJoin optimization (users x orders):")
    for m in ["nested", "hash", "merge"]:
        j = JoinOp(scan_u, scan_o, m)
        print(f"  {j}")
    best = optimize_join(scan_u, scan_o)
    print(f"  Best: {best}")
