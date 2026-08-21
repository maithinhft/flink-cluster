package generator.events;

import generator.common.EnvLoader;

public class EventConfig {
    public String bootstrapServers = EnvLoader.get("SERVER_IP", "127.0.0.1") + ":" + EnvLoader.get("KAFKA_PORT", "9092");
    public String topic = "events";
    public long numEvents = 1_000_000L;
    public double dirtyRate = 0.05;
    public double lateEventRate = 0.05;
    public int numEntities = 100_000;
    public double dataSkew = 0.5;
    public int workers = 4;

    public static EventConfig parse(String[] args) {
        EventConfig config = new EventConfig();
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--bootstrap-servers":
                    config.bootstrapServers = args[++i];
                    break;
                case "--topic":
                    config.topic = args[++i];
                    break;
                case "--num-events":
                    config.numEvents = Long.parseLong(args[++i]);
                    break;
                case "--dirty-rate":
                    config.dirtyRate = Double.parseDouble(args[++i]);
                    break;
                case "--late-event-rate":
                    config.lateEventRate = Double.parseDouble(args[++i]);
                    break;
                case "--num-entities":
                    config.numEntities = Integer.parseInt(args[++i]);
                    break;
                case "--data-skew":
                    config.dataSkew = Double.parseDouble(args[++i]);
                    break;
                case "--workers":
                    config.workers = Integer.parseInt(args[++i]);
                    break;
            }
        }
        return config;
    }

    public void validate() {
        if (numEvents <= 0)
            throw new IllegalArgumentException("num-events must be > 0");
        if (dirtyRate < 0 || dirtyRate > 1)
            throw new IllegalArgumentException("dirty-rate must be 0-1");
        if (lateEventRate < 0 || lateEventRate > 1)
            throw new IllegalArgumentException("late-event-rate must be 0-1");
    }
}
