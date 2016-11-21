package monitorBolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import org.json.JSONObject;
//import org.json.JSONException;

public class ApiAlertBoltLogParse extends BaseBasicBolt {

	//public void cleanup() {}

	/**
	 * The bolt will receive the line from the
	 * nginx log and process it
	 */
	
	@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			String line = input.getString(0);
			try {
				String decoded = new String(line);
				JSONObject json = new JSONObject(decoded);
				collector.emit("logParse", new Values(
							json.getString("api_id"),
							json.getInt("request_time"),
							json.getInt("status")
							));
			} catch (Exception e) {
				System.out.println("Caught JSONException: " + e.getMessage());
			}
		}


	/**
	 * The bolt will emit the field minute,sum(elapse),httpcode,request 
	 */
	@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream("logParse",new Fields("apiId","elapse","httpcode"));
		}
}
