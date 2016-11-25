
  package monitorBolt;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Arrays;
import java.lang.Float;
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
import monitorUtils.*;
public class ApiAlertBoltAbnormal extends BaseRichBolt {
    
    private OutputCollector collector;
    
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    //此bolt的所有task都会每隔一段时间收到一个来自__systemd的__tick stream的tick tuple
    public Map<String, Object> getComponentConfiguration(Float period) {
        Config conf = new Config();
        conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS,period);
        return conf;
    }
    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
    
    @Override
    public void execute(Tuple input) {
        ArrayList<ApiAlertUtilsConfigModel> configs = 
            (ArrayList<ApiAlertUtilsConfigModel>) input.getValueByField("configs");
        ArrayList<Map<String,ApiAlertUtilsApiIdModel>> groupList = 
            (ArrayList<Map<String,ApiAlertUtilsApiIdModel>>) input.getValueByField("groupList");
        ArrayList<Map<String,String>> abnormalList = ApiAlertUtilsAbnormal.UtilsAbnormal(groupList,configs);
        collector.emit("abnormalList",new Values(abnormalList));
    }	
    
    /*
     * The bolt will emit the field minute,sum(requestTime),httpCode,pvCount 
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declareStream("abnormalList",new Fields("abnormalList"));
    }
}
