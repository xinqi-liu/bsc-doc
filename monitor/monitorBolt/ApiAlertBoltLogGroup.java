package monitorBolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject ;

import backtype.storm.task.TopologyContext;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import backtype.storm.Config;
import backtype.storm.tuple.Values;
import backtype.storm.Constants;
import backtype.storm.tuple.Fields;


 /*
 * 此bolt每分钟聚合一次nginx日志，然后缓存进groupList，并更新groupList的index
 */
public class ApiAlertBoltLogGroup extends BaseRichBolt {
	public ArrayList<Map<String,Map<String, Map<String, Long>>>> groupList;
	private OutputCollector collector;
	
	//@Override
	//public void cleanup() {}
	
	/**该方法只执行一次，在最开始的时候执行，在execute()方法之前执行**/
	@Override 
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {  
		this.collector = collector;  
		this.groupList = new ArrayList<Map<String,Map<String, Map<String, Long>>>>();
	};  	

	@Override
	//此bolt的所有task都会每隔一段时间收到一个来自__systemd的__tick stream的tick tuple
	public Map<String, Object> getComponentConfiguration() {
		Config conf = new Config();
		conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS,60);
		return conf;
	};

	@Override
	public void execute(Tuple input) {
		Map<String, Long> dataMap= new HashMap<String, Long>();
		Map<String, Long> dataMapTotal= new HashMap<String, Long>();
		Map<String, Map<String, Long>> httpcodeMap = new HashMap<String, Map<String, Long>>();
		Map<String,Map<String, Map<String, Long>>> apiMap = new HashMap<String,Map<String, Map<String, Long>>>();
		Long request;
		Long sumElapse;
		Long requestTotal;
		Long sumElapseTotal;	
		
	
		String apiId = input.getStringByField("apiId");
		String httpcode = input.getStringByField("httpcode");
		Integer elapse = input.getIntegerByField("elapse");
		
		if(groupList.get(0).get(apiId).get(httpcode)!=null) {
			request = groupList.get(0).get(apiId).get(httpcode).get("request")+1;
			sumElapse = groupList.get(0).get(apiId).get(httpcode).get("sumElapse")+elapse;
		}
		else {
			request = (long) 1;
			sumElapse = (long) elapse;
		}
		if(groupList.get(0).get(apiId).get("total")!=null) {
			requestTotal = groupList.get(0).get(apiId).get("total").get("request")+1;	
			sumElapseTotal = groupList.get(0).get(apiId).get("total").get("sumElapse")+elapse;
		}
		else {
			requestTotal = (long) 1;
			sumElapseTotal = (long) elapse;
		}
		/**
		 * If the apiId dosn't exist in the map we will create
		 * this, if not We will accumulate 
		 */
		//step1：更新数据
        dataMap.put("request",request); 
		dataMap.put("sumElapse",sumElapse); 
		dataMapTotal.put("request",requestTotal);
		dataMapTotal.put("sumElapse",sumElapseTotal); 
		//step2：新增或覆盖此httpcode/api以及总的请求数和响应时间
		if(groupList.get(0).get(apiId)!=null) {
			httpcodeMap.putAll(groupList.get(0).get(apiId));
			httpcodeMap.put(httpcode,dataMap);
			httpcodeMap.put("total",dataMapTotal);
			apiMap.putAll(groupList.get(0));
			apiMap.put(apiId,httpcodeMap);
                }
		else {
			httpcodeMap.put(httpcode,dataMap);
			httpcodeMap.put("total",dataMapTotal);
			apiMap.put(apiId,httpcodeMap);
		}
		//step3:更新list
		if (this.isTickTuple(input)) {
			//groupList每个元素index加1
			groupList.addAll(1,groupList);
                        //删除index>=30的httpcodeMap
                        if(groupList.size()>30) {
                        	groupList.remove(30);
			}
			//替换第一个元素
			groupList.add(0,apiMap);
			//emit
			collector.emit("groupList",new Values(groupList));
		}
		else {
			//更新groupList
			groupList.add(0,apiMap);
		}
	}
	
	protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
                declarer.declareStream("groupList",new Fields("groupList"));
        }

}
