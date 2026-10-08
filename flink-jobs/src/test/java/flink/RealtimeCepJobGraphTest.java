package flink;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamNode;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RealtimeCepJobGraphTest {

    private static StreamGraph graph(String... args) {
        ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        RealtimeCepJob.configureEnvironment(env, params);
        env.getConfig().disableGenericTypes();
        RealtimeCepJob.build(env, params);
        return env.getStreamGraph();
    }

    @Test
    void everyStatefulOperatorHasAStableUidAndNoKryoTypes() {
        for (String dynamic : new String[]{"true", "false"}) {
            StreamGraph g = graph("--use.dynamic.source", dynamic, "--bootstrap.enabled", "false",
                    "--checkpoint.interval.ms", "10000");
            Set<String> uids = new HashSet<>();
            for (StreamNode n : g.getStreamNodes()) {
                if (n.getTransformationUID() != null) {
                    uids.add(n.getTransformationUID());
                }
            }
            assertTrue(uids.containsAll(Set.of("src-control-schema", "src-control-rule", "map-control", "src-events",
                    "op-validate", "op-engine", "sink-result", "sink-dlq")), uids.toString());
            assertEquals(128, g.getExecutionConfig().getMaxParallelism());
            assertTrue(g.getCheckpointConfig().isUnalignedCheckpointsEnabled());
        }
    }
}
