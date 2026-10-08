package flink.index;

import flink.compile.CompiledRule;
import flink.compile.Values;
import flink.model.ValidatedEvent;
import org.roaringbitmap.FastAggregation;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Inverted index over rule slots used to discard most candidate rules in near-constant time.
 *
 * <ul>
 *   <li>FULL: every rule is a candidate (baseline for benchmarks).</li>
 *   <li>TRIGGER: candidates are rules whose {@code trigger_events} contain the event type.</li>
 *   <li>DF2: each rule is posted under its two rarest necessary clauses by document frequency
 *       (axis A and axis B; the trigger list is one of the clauses). Query = union of the postings matched
 *       on axis A, intersected with the union of postings matched on axis B (plus rules without axis B).</li>
 * </ul>
 * The index only removes candidates: every candidate is still fully evaluated, so it never changes results.
 */
public final class InvertedIndex {

    public enum Mode { FULL, TRIGGER, DF2 }

    public static final String TRIGGER_FIELD = "__trigger";

    private static final class Assignment {
        final CompiledRule rule;
        final Set<NeedExtractor.Key> dfKeys;
        Set<NeedExtractor.Key> axisA;
        Set<NeedExtractor.Key> axisB;

        Assignment(CompiledRule rule, Set<NeedExtractor.Key> dfKeys) {
            this.rule = rule;
            this.dfKeys = dfKeys;
        }
    }

    private final Mode mode;
    private final RoaringBitmap all = new RoaringBitmap();
    private final Map<String, RoaringBitmap> triggerPosting = new HashMap<>();
    private final Map<String, Map<String, RoaringBitmap>> axisA = new HashMap<>();
    private final Map<String, Map<String, RoaringBitmap>> axisB = new HashMap<>();
    private final RoaringBitmap wildcardB = new RoaringBitmap();
    private final Map<NeedExtractor.Key, Long> df = new HashMap<>();
    private final Map<Integer, Assignment> assignments = new HashMap<>();
    private int updatesSinceRebuild;
    private boolean rebuildPending;
    private long rebuilds;

    public InvertedIndex(Mode mode) {
        this.mode = mode;
    }

    public Mode mode() {
        return mode;
    }

    public int size() {
        return all.getCardinality();
    }

    public long rebuilds() {
        return rebuilds;
    }

    public void add(CompiledRule rule) {
        int idx = rule.idx();
        if (rule.triggers().isEmpty()) {
            return;
        }
        all.add(idx);
        for (String t : rule.triggers()) {
            triggerPosting.computeIfAbsent(t, k -> new RoaringBitmap()).add(idx);
        }
        if (mode != Mode.DF2) {
            return;
        }
        Set<NeedExtractor.Key> keys = new HashSet<>();
        NeedExtractor.allKeys(rule.root(), keys);
        for (String t : rule.triggers()) {
            keys.add(new NeedExtractor.Key(TRIGGER_FIELD, t));
        }
        for (NeedExtractor.Key k : keys) {
            df.merge(k, 1L, Long::sum);
        }
        Assignment a = new Assignment(rule, keys);
        assign(a);
        assignments.put(idx, a);
        noteUpdate();
    }

    public void remove(CompiledRule rule) {
        int idx = rule.idx();
        if (!all.contains(idx)) {
            return;
        }
        all.remove(idx);
        for (String t : rule.triggers()) {
            RoaringBitmap b = triggerPosting.get(t);
            if (b != null) {
                b.remove(idx);
                if (b.isEmpty()) {
                    triggerPosting.remove(t);
                }
            }
        }
        if (mode != Mode.DF2) {
            return;
        }
        Assignment a = assignments.remove(idx);
        if (a == null) {
            return;
        }
        unpost(a, idx);
        for (NeedExtractor.Key k : a.dfKeys) {
            df.computeIfPresent(k, (kk, v) -> v <= 1 ? null : v - 1);
        }
        noteUpdate();
    }

    private void noteUpdate() {
        updatesSinceRebuild++;
        if (updatesSinceRebuild > Math.max(1000, assignments.size() / 10)) {
            rebuildPending = true;
        }
    }

    private long dfOf(NeedExtractor.Key k) {
        Long v = df.get(k);
        return v == null ? 0L : v;
    }

    private void assign(Assignment a) {
        List<Set<NeedExtractor.Key>> clauses = new ArrayList<>(NeedExtractor.clauses(a.rule.root(), this::dfOf));
        Set<NeedExtractor.Key> trig = new HashSet<>();
        for (String t : a.rule.triggers()) {
            trig.add(new NeedExtractor.Key(TRIGGER_FIELD, t));
        }
        clauses.add(trig);
        clauses.sort(Comparator.<Set<NeedExtractor.Key>>comparingLong(c -> NeedExtractor.cost(c, this::dfOf))
                .thenComparingInt(Set::size));
        a.axisA = clauses.get(0);
        a.axisB = clauses.size() > 1 ? clauses.get(1) : null;
        int idx = a.rule.idx();
        post(axisA, a.axisA, idx);
        if (a.axisB == null) {
            wildcardB.add(idx);
        } else {
            post(axisB, a.axisB, idx);
        }
    }

    private void unpost(Assignment a, int idx) {
        unpost(axisA, a.axisA, idx);
        if (a.axisB == null) {
            wildcardB.remove(idx);
        } else {
            unpost(axisB, a.axisB, idx);
        }
    }

    private static void post(Map<String, Map<String, RoaringBitmap>> axis, Set<NeedExtractor.Key> clause, int idx) {
        for (NeedExtractor.Key k : clause) {
            axis.computeIfAbsent(k.field(), f -> new HashMap<>())
                    .computeIfAbsent(k.value(), v -> new RoaringBitmap())
                    .add(idx);
        }
    }

    private static void unpost(Map<String, Map<String, RoaringBitmap>> axis, Set<NeedExtractor.Key> clause, int idx) {
        if (clause == null) {
            return;
        }
        for (NeedExtractor.Key k : clause) {
            Map<String, RoaringBitmap> byValue = axis.get(k.field());
            if (byValue == null) {
                continue;
            }
            RoaringBitmap b = byValue.get(k.value());
            if (b != null) {
                b.remove(idx);
                if (b.isEmpty()) {
                    byValue.remove(k.value());
                    if (byValue.isEmpty()) {
                        axis.remove(k.field());
                    }
                }
            }
        }
    }

    public void rebuild() {
        axisA.clear();
        axisB.clear();
        wildcardB.clear();
        for (Assignment a : assignments.values()) {
            assign(a);
        }
        updatesSinceRebuild = 0;
        rebuildPending = false;
        rebuilds++;
    }

    public int triggerCandidates(ValidatedEvent ev) {
        if (ev.eventType == null) {
            return 0;
        }
        RoaringBitmap b = triggerPosting.get(ev.eventType);
        return b == null ? 0 : b.getCardinality();
    }

    public RoaringBitmap candidates(ValidatedEvent ev) {
        switch (mode) {
            case FULL:
                return all;
            case TRIGGER: {
                RoaringBitmap b = ev.eventType == null ? null : triggerPosting.get(ev.eventType);
                return b == null ? new RoaringBitmap() : b;
            }
            default:
                if (rebuildPending) {
                    rebuild();
                }
                RoaringBitmap a = unionMatched(axisA, ev, null);
                if (a.isEmpty()) {
                    return a;
                }
                RoaringBitmap b = unionMatched(axisB, ev, wildcardB);
                return RoaringBitmap.and(a, b);
        }
    }

    private static RoaringBitmap unionMatched(Map<String, Map<String, RoaringBitmap>> axis, ValidatedEvent ev,
                                              RoaringBitmap extra) {
        List<RoaringBitmap> hits = new ArrayList<>(8);
        if (extra != null && !extra.isEmpty()) {
            hits.add(extra);
        }
        for (Map.Entry<String, Map<String, RoaringBitmap>> e : axis.entrySet()) {
            String value;
            if (TRIGGER_FIELD.equals(e.getKey())) {
                value = ev.eventType;
            } else {
                int i = ev.find(e.getKey());
                value = i < 0 ? null : Values.canonEvent(ev, i);
            }
            if (value == null) {
                continue;
            }
            RoaringBitmap b = e.getValue().get(value);
            if (b != null) {
                hits.add(b);
            }
        }
        if (hits.isEmpty()) {
            return new RoaringBitmap();
        }
        if (hits.size() == 1) {
            return hits.get(0);
        }
        return FastAggregation.or(hits.iterator());
    }

    public int postingKeys() {
        int n = 0;
        for (Map<String, RoaringBitmap> m : axisA.values()) {
            n += m.size();
        }
        for (Map<String, RoaringBitmap> m : axisB.values()) {
            n += m.size();
        }
        return n;
    }

    public int rulesWithoutAxisB() {
        return wildcardB.getCardinality();
    }
}
