package spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Map;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class FlightsDataReader extends BaseRichSpout {
    private SpoutOutputCollector collector;
    private FileReader flightsFileReader;
    private boolean completed = false;

    @Override
    public void ack(Object msgId) {
		System.out.println("OK:"+msgId);
	}

    @Override
	public void close() {}

    @Override
	public void fail(Object msgId) {
		System.out.println("FAIL:"+msgId);
	}

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.flightsFileReader = new FileReader(conf.get("FlightsFile").toString());
        } catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+conf.get("FlightsFile")+"]");
        }
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        if(completed) {
            try {
                Thread.sleep(1000);
            } catch (Exception e) {}
            return;
        }

        JsonNode states = readFlightsData();

        try {
            for(JsonNode state : states) {
                String[] values = new String[state.size()];
                for(int i = 0; i < values.length; i++) {
                    values[i] = state.get(i).asText();
                }
                this.collector.emit(new Values(values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9], values[10], values[11], values[12], values[13], values[14], values[15], values[16]));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple",e);
		}finally{
			completed = true;
		}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("transponder address", "call sign",
            "origin country", "last timestamp1", "last timestamp2", "longitude", "latitude", 
            "altitude (barometric)", "surface or air", "velocity (meters/sec)", "degree north = 0",
            "vertical rate", "sensors", "altitude (geometric)", "transponder code", "sprcial purpose", 
            "origin"
        ));
    }
    
    /**
     * Reads the data from the flights file and extracts the data from the JSON
     * @return JsonNode containing the data from the flights file
     */
    private JsonNode readFlightsData() {
        ObjectMapper mapper = new ObjectMapper();
        try {
            // Read the file
            StringBuilder jsonString = new StringBuilder();
            String line = "";
            BufferedReader reader = new BufferedReader(flightsFileReader);
            while ((line = reader.readLine()) != null) {
                jsonString.append(line);
            }

            // Extract the data from the JSON
            JsonNode root = mapper.readTree(jsonString.toString());
            return root.get("states");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
