package flink.index;

import flink.compile.Node;
import flink.compile.Op;
import flink.compile.RawPredNode;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.ToLongFunction;

/**
 * Extracts the "necessary predicates" of a condition tree: a list of clauses, each clause being a set of
 * (field, value) keys, such that the rule can only be true if, for every clause, the event matches at
 * least one key of that clause. Used to build the inverted index without ever producing false negatives.
 *
 * <pre>
 * need(f EQ v)      = [{(f,v)}]
 * need(f IN [..])   = [{(f,v1),...,(f,vk)}]
 * need(AND(c..))    = concat(need(ci))
 * need(OR(c..))     = [ union of the rarest clause of each child ]   if every child has a clause, else []
 * need(other)       = []   (NOT, ranges, aggregates, linear)
 * </pre>
 */
public final class NeedExtractor {

    public record Key(String field, String value) {
    }

    private NeedExtractor() {
    }

    public static List<Set<Key>> clauses(Node n, ToLongFunction<Key> df) {
        if (n instanceof RawPredNode) {
            RawPredNode r = (RawPredNode) n;
            if (r.op() == Op.EQ && r.operand().canon() != null) {
                Set<Key> s = new HashSet<>();
                s.add(new Key(r.field(), r.operand().canon()));
                return Collections.singletonList(s);
            }
            if (r.op() == Op.IN && !r.inValues().isEmpty()) {
                Set<Key> s = new HashSet<>();
                for (String v : r.inValues()) {
                    s.add(new Key(r.field(), v));
                }
                return Collections.singletonList(s);
            }
            return Collections.emptyList();
        }
        if (n instanceof Node.And) {
            List<Set<Key>> out = new ArrayList<>();
            for (Node c : ((Node.And) n).children()) {
                out.addAll(clauses(c, df));
            }
            return out;
        }
        if (n instanceof Node.Or) {
            Set<Key> union = new HashSet<>();
            for (Node c : ((Node.Or) n).children()) {
                List<Set<Key>> cc = clauses(c, df);
                if (cc.isEmpty()) {
                    return Collections.emptyList();
                }
                union.addAll(rarest(cc, df));
            }
            return Collections.singletonList(union);
        }
        if (n instanceof Node.Const && !((Node.Const) n).value()) {
            return Collections.singletonList(new HashSet<>());
        }
        return Collections.emptyList();
    }

    public static Set<Key> rarest(List<Set<Key>> clauses, ToLongFunction<Key> df) {
        Set<Key> best = null;
        long bestCost = Long.MAX_VALUE;
        for (Set<Key> c : clauses) {
            long cost = cost(c, df);
            if (cost < bestCost || (cost == bestCost && best != null && c.size() < best.size())) {
                best = c;
                bestCost = cost;
            }
        }
        return best;
    }

    public static long cost(Collection<Key> clause, ToLongFunction<Key> df) {
        long s = 0;
        for (Key k : clause) {
            s += df.applyAsLong(k);
        }
        return s;
    }

    public static void allKeys(Node n, Set<Key> out) {
        if (n instanceof RawPredNode) {
            RawPredNode r = (RawPredNode) n;
            if (r.op() == Op.EQ && r.operand().canon() != null) {
                out.add(new Key(r.field(), r.operand().canon()));
            } else if (r.op() == Op.IN) {
                for (String v : r.inValues()) {
                    out.add(new Key(r.field(), v));
                }
            }
        } else if (n instanceof Node.And) {
            for (Node c : ((Node.And) n).children()) {
                allKeys(c, out);
            }
        } else if (n instanceof Node.Or) {
            for (Node c : ((Node.Or) n).children()) {
                allKeys(c, out);
            }
        }
    }
}
