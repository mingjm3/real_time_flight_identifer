import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import bolts.AirlineSorter;
import bolts.HubIdentifier;
import spouts.FlightsDataReader;

public class TopologyMain {
    public static void main(String[] args) throws InterruptedException {
        // Define the topology
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("flights-reader", new FlightsDataReader());
        builder.setBolt("hub-identifier", new HubIdentifier(), 1).shuffleGrouping("flights-reader");
        builder.setBolt("airline-sorter", new AirlineSorter(), 1).fieldsGrouping("hub-identifier", new Fields("airport.city", "airport.code", "call sign"));

        // Configuration
        Config conf = new Config();
        conf.setDebug(true);
        conf.put("FlightsFile", args[0]);
        conf.put("AirportsData", args[1]);

        // Run
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("Flight-Data-Toplogie", conf, builder.createTopology());
        Thread.sleep(1000);
        cluster.shutdown();
    }
}
