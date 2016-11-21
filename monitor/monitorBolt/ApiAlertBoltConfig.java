package monitorBolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.Config;
import backtype.storm.Constants;

import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.JsonNode ;
import com.mashape.unirest.http.HttpResponse;

public class ApiAlertBoltConfig extends BaseBasicBolt {
	//此bolt的所有task都会每隔一段时间收到一个来自__systemd的__tick stream的tick tuple
	public Map<String, Object> getComponentConfiguration() {
            Config conf = new Config();
            conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS,300);
            return conf;
        }
	

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		if (this.isTickTuple(input)) {
			try{
				HttpResponse<String> userConfig = Unirest.get("http://10.211.55.4:8002/internal/alert/config").asString();
				collector.emit("userConfig",new Values(userConfig));
			}catch(Exception e){
				System.out.println(e);
			}
		};
		
	}
	
	protected static boolean isTickTuple(Tuple tuple) {
		return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
			&& tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
	}

	/**
	 * The bolt will emit the field minute,sum(elapse),httpcode,request 
	 */
	@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declareStream("userConfig",new Fields("userConfig"));
		}
}
