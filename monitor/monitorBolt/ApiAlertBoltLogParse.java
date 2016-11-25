package monitorBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.JSONObject;
//import org.json.JSONException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.lang.Float;
import monitorUtils.*;
public class ApiAlertBoltLogParse extends BaseBasicBolt {
    //public void cleanup() {}
    /**
     * The bolt will receive the line from the
     * nginx log and process it
     */
    
    @Override
        public void execute(Tuple input, BasicOutputCollector collector) {
            String line = input.getString(0);
            Map<String,String> log = ApiAlertUtilsLogParse.UtilsLogParse(line);
            collector.emit("log", new Values(log));
        }
    /**
     * The bolt will emit the field sum(requestTime),httpCode,pvCount 
     */
    @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream("log",new Fields("log"));
        }
}
