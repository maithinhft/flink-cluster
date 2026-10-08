package flink.engine;

import flink.registry.MetricRegistry;
import flink.testutil.TestSupport;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static flink.testutil.TestSupport.deleteRule;
import static flink.testutil.TestSupport.rule;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

class RuleCatalogTest {

    private static final String R1 = "{\"trigger_events\":[\"purchase\"],\"condition\":{\"type\":\"AGGREGATION\","
            + "\"field\":\"total_amount\",\"function\":\"SUM\",\"window\":{\"lookback_seconds\":3600},\"operator\":\"GT\",\"value\":1}}";
    private static final String R1_THRESHOLD = R1.replace("\"value\":1}", "\"value\":99}");

    private RuleCatalog catalog;

    @BeforeEach
    void setUp() {
        catalog = new RuleCatalog(TestSupport.config());
    }

    @Test
    void newerVersionsReplaceAndStaleOrDuplicateOnesAreIgnored() {
        assertEquals(RuleCatalog.Status.APPLIED, catalog.apply(rule("r", 1, R1, 0), 0L).status);
        assertEquals(RuleCatalog.Status.APPLIED, catalog.apply(rule("r", 3, R1_THRESHOLD, 0), 0L).status);
        assertEquals(RuleCatalog.Status.IGNORED_STALE, catalog.apply(rule("r", 2, R1, 0), 0L).status);
        assertEquals(RuleCatalog.Status.IGNORED_STALE, catalog.apply(rule("r", 3, R1, 0), 0L).status);
        assertEquals(3, catalog.rule("r").version());
        assertEquals(1, catalog.ruleCount());
    }

    @Test
    void deletingAnOlderVersionRowDoesNotRemoveTheCurrentRule() {
        catalog.apply(rule("r", 1, R1, 0), 0L);
        catalog.apply(rule("r", 2, R1, 0), 0L);
        assertEquals(RuleCatalog.Status.IGNORED_STALE, catalog.apply(deleteRule("r", 1), 0L).status);
        assertNotNull(catalog.rule("r"));
        assertEquals(RuleCatalog.Status.REMOVED, catalog.apply(deleteRule("r", 2), 0L).status);
        assertNull(catalog.rule("r"));
        assertEquals(RuleCatalog.Status.IGNORED_STALE, catalog.apply(rule("r", 2, R1, 0), 0L).status,
                "replayed upsert of the deleted version must not resurrect the rule");
        assertEquals(RuleCatalog.Status.APPLIED, catalog.apply(rule("r", 3, R1, 0), 0L).status);
    }

    @Test
    void disablingRemovesAndReenablingWithNewVersionRestores() {
        catalog.apply(rule("r", 1, R1, 0), 0L);
        assertEquals(RuleCatalog.Status.REMOVED, catalog.apply(rule("r", 2, R1, 0, false, Long.MIN_VALUE), 0L).status);
        assertEquals(0, catalog.ruleCount());
        assertEquals(0, catalog.registry().activeCount());
        assertEquals(RuleCatalog.Status.APPLIED, catalog.apply(rule("r", 3, R1, 0), 0L).status);
        assertEquals(1, catalog.registry().activeCount());
    }

    @Test
    void rejectedDefinitionKeepsPreviousVersion() {
        catalog.apply(rule("r", 1, R1, 0), 0L);
        RuleCatalog.Result bad = catalog.apply(rule("r", 2, "{\"trigger_events\":[\"purchase\"],\"condition\":"
                + "{\"type\":\"AGGREGATION\",\"field\":\"x\",\"function\":\"MEDIAN\",\"operator\":\"GT\",\"value\":1}}", 0), 0L);
        assertEquals(RuleCatalog.Status.REJECTED, bad.status);
        assertNotNull(bad.error);
        assertEquals(1, catalog.rule("r").version());
        assertEquals(RuleCatalog.Status.REJECTED, catalog.apply(rule("r", 3, "not json", 0), 0L).status);
    }

    @Test
    void sharedSpecStaysActiveAcrossVersionUpgradeAndBetweenRules() {
        catalog.apply(rule("a", 1, R1, 0), 0L);
        catalog.apply(rule("b", 1, R1_THRESHOLD, 0), 0L);
        MetricRegistry reg = catalog.registry();
        assertEquals(1, reg.activeCount());
        long key = catalog.rule("a").acquiredSpecKeys()[0];
        long storage = reg.entry(key).storageKey();
        assertEquals(2, reg.entry(key).refCount());

        catalog.apply(rule("a", 2, R1_THRESHOLD, 0), 0L);
        assertEquals(2, reg.entry(key).refCount());
        assertEquals(storage, reg.entry(key).storageKey(), "upgrade must not re-incarnate the shared spec");
        assertEquals(0, reg.retiredCount());

        catalog.apply(deleteRule("a", 2), 0L);
        catalog.apply(deleteRule("b", 1), 0L);
        assertEquals(0, reg.activeCount());
        assertEquals(1, reg.retiredCount());
    }

    @Test
    void ruleSlotsAreReusedAfterDeletion() {
        catalog.apply(rule("a", 1, R1, 0), 0L);
        int idx = catalog.rule("a").idx();
        catalog.apply(deleteRule("a", 1), 0L);
        catalog.apply(rule("b", 1, R1, 0), 0L);
        assertEquals(idx, catalog.rule("b").idx());
        assertNull(catalog.ruleAt(99));
    }
}
