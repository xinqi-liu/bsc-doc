package monitorBolt;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.Iterator;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.JsonNode ;
import com.mashape.unirest.http.HttpResponse;

public class ApiAlertBoltAlert extends BaseBasicBolt {

	@Override
	public void cleanup() {}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {}

	private void sendAlert(ArrayList<ArrayList<String>> abnormalList) {
		Iterator<ArrayList<String>> it = abnormalList.iterator();
		while(it.hasNext()) {
			ArrayList<String>  abnormal = it.next();
			String body = "{  \"api_id\": \""+abnormal.get(0)+"\",\n  \"strategy_id\": \""+abnormal.get(1)+"\",  \"indicator_id\": \""+abnormal.get(2)+"\",\n  \"time\": \""+abnormal.get(3)+"\"}";
			JsonNode bodyJson = new JsonNode(body) ;
			try{
				HttpResponse<JsonNode> response = Unirest.post("http://10.211.55.4:8002/internal/alert")
					.header("content-type", "application/json")
					.body(bodyJson)
					.asJson();
			}catch(Exception e){
				System.out.println(e);
			}
		}
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		ArrayList<ArrayList<String>> abnormalList = (ArrayList<ArrayList<String>>)input.getValueByField("abnormalList");
		sendAlert(abnormalList);
	}
}
