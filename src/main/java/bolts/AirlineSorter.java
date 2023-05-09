package bolts;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

public class AirlineSorter extends BaseRichBolt {
    // map<airportCode, map<callSign, occurrences>>
    private Map<String, Map<String, Integer>> cnt;

    // map<airportCode, airportCity>
    private Map<String, String> codeToCity;

    @Override
    public void cleanup() {
        System.out.println("-- Flight Counter [airline-sorter-7] --");
        for(Map.Entry<String, Map<String, Integer>> entry : cnt.entrySet()) {
            int total = 0;
            String airportCode = entry.getKey();
            String airportCity = codeToCity.get(airportCode);
            System.out.println(String.format("At Airport: %s(%s)", airportCode, airportCity));
            for(Map.Entry<String, Integer> pair : entry.getValue().entrySet()) {
                String callSign = pair.getKey();
                int occurrences = pair.getValue();
                total += occurrences;
                System.out.println(String.format("\t%s: %s", callSign, occurrences));
            }
            System.out.println(String.format("\ttotal #flights = %s %n", total));
        }
    }

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.cnt = new HashMap<String, Map<String, Integer>>();
        this.codeToCity = new HashMap<String, String>();
    }

    @Override
    public void execute(Tuple input) {
        String airportCode = input.getStringByField("airport.code");
        String airportCity = input.getStringByField("airport.city");
        String callSign = input.getStringByField("call sign");

        if(!codeToCity.containsKey(airportCode)) {
            codeToCity.put(airportCode, airportCity);
        }

        if(!cnt.containsKey(airportCode)) {
            cnt.put(airportCode, new HashMap<String, Integer>());
        }

        cnt.get(airportCode).put(callSign, cnt.get(airportCode).getOrDefault(callSign, 0)+1);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
}