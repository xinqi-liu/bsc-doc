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
import monitorUtils.*;
public class ApiAlertBoltAlert extends BaseBasicBolt {
    public String PostUrl;
    @Override
    public void cleanup() {}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {}
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        ArrayList<Map<String,String>> abnormalList = (ArrayList<Map<String,String>>)input.getValueByField("abnormalList");
        ApiAlertUtilsAlert.UtilsAlert(abnormalList,PostUrl);
    }
}
