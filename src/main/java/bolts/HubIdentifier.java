package bolts;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class HubIdentifier extends BaseRichBolt{
    private static final double PROXIMITY = 20;
    private static final double CRUISING_ALTITUDE = 6000;
    private static final double CRUISING_VELOCITY = 83.3;
    private static final double LATITUDE_TO_MILES = 70;
    private static final double LONGITUDE_TO_MILES = 45;
    private List<String[]> airports;
    private OutputCollector collector;

    @Override
    public void cleanup() {}

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.airports = getAirports((String)stormConf.get("AirportsData"));
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        try {
            Double[] flightLocation = new Double[] {Double.parseDouble(input.getStringByField("latitude")), Double.parseDouble(input.getStringByField("longitude"))}; 
            String callSign = input.getStringByField( "call sign");
            Double altitude = Double.parseDouble(input.getStringByField("altitude (barometric)"));
            Double velocity = Double.parseDouble(input.getStringByField("velocity (meters/sec)"));
            Double verticalRate = Double.parseDouble(input.getStringByField("vertical rate"));

            for(String[] airport : airports) {
                Double[] airportLocation = new Double[] {Double.parseDouble(airport[2]), Double.parseDouble(airport[3])};
                if(isBelongToAirport(flightLocation, airportLocation, altitude, velocity, verticalRate)) {
                    collector.emit(new Values(airport[0], airport[1], getFlightAirlineCode(callSign)));
                    break;
                }
            }
        } catch (NumberFormatException e) {}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("airport.city", "airport.code", "call sign"));
    }

    /**
     * Get airports from a file and return a list of airports.
     * @param path file path
     * @return list of airports (airports_city, airport_code, latitude, longitude)
     */
    private List<String[]> getAirports(String path) {
        this.airports = new ArrayList<String[]>();
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(path));
            String line = reader.readLine();
            while (line!= null) {
                if(line.equals("")) {
                    line = reader.readLine();
                    continue;
                }

                String[] airport = line.split(",");
                airports.add(airport);
                line = reader.readLine();
            }
            reader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return airports;
    }

    /**
     * Check if a flight is belong to an airport.
     * @param flightLocation flight location [latitude, longitude]
     * @param airportLocation airport location [latitude, longitude]
     * @param altitude flight altitude
     * @param velocity flight velocity
     * @param verticalRate flight vertical rate
     * @return true if flight is belong to the airport, false otherwise
     */
    
    private boolean isBelongToAirport(Double[] flightLocation, Double[] airportLocation, Double altitude, Double velocity, Double verticalRate) {
        if(verticalRate == 0) {return false;}
        if(altitude > CRUISING_ALTITUDE) {return false;}
        if(velocity > CRUISING_VELOCITY) {return false;}
        double diffLatitude = Math.abs(flightLocation[0] - airportLocation[0]);
        double diffLongitude = Math.abs(flightLocation[1] - airportLocation[1]);
        return diffLatitude * LATITUDE_TO_MILES < PROXIMITY && diffLongitude * LONGITUDE_TO_MILES < PROXIMITY;
    }

    /**
     * Get the airline code from a call sign.
     * @param callSign
     * @return the airline code
     */
    private String getFlightAirlineCode(String callSign) {
        if(callSign.length() <= 3) {return callSign;}
        return callSign.substring(0, 3);
    }
}
