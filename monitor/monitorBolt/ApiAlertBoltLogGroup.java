package monitorBolt;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject ;
import java.lang.Float;
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
//引入slf4j接口的Logger和LoggerFactory
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import monitorUtils.*;
 /*
 * 此bolt每分钟聚合一次nginx日志，然后缓存进groupList，并更新groupList的index
 */
public class ApiAlertBoltLogGroup extends BaseRichBolt {
    public ArrayList<Map<String,ApiAlertUtilsApiIdModel>> groupList;
    private OutputCollector collector;
    final static Logger logger = LoggerFactory.getLogger(ApiAlertBoltLogGroup.class);
    //@Override
    //public void cleanup() {}
    
    /**该方法只执行一次，在最开始的时候执行，在execute()方法之前执行**/
    @Override 
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {  
        this.collector = collector;  
        this.groupList = new ArrayList<Map<String,ApiAlertUtilsApiIdModel>>();
    }  	
    @Override
    //此bolt的所有task都会每隔一段时间收到一个来自__systemd的__tick stream的tick tuple
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS,60);
        return conf;
    }
    
    @Override
    public void execute(Tuple input) {
        Map<String,String> log = (Map<String,String>)input.getValueByField("log");
        Map<String,ApiAlertUtilsApiIdModel> apiMap = groupList.get(0);
        if(this.isTickTuple(input)){
            groupList.addAll(1,groupList);
            //删除index>=30的httpCodeMap
            if(groupList.size()>30) {
                for(int i=30; i<groupList.size(); i++) {
                    groupList.remove(i);
                }   
            }   
            //替换第一个元素
            groupList.add(0,apiMap);
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
