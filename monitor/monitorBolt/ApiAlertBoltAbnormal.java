package monitorBolt;

import org.json.JSONObject;
import org.json.JSONException;
import org.json.JSONArray;
import org.json.JSONStringer;
import org.json.JSONTokener;

import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Arrays;

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

public class ApiAlertBoltAbnormal extends BaseRichBolt {
    private OutputCollector collector;
    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }
    //此bolt的所有task都会每隔一段时间收到一个来自__systemd的__tick stream的tick tuple
    public Map<String, Object> getComponentConfiguration() {
        Config conf = new Config();
        conf.put(conf.TOPOLOGY_TICK_TUPLE_FREQ_SECS,300);
        return conf;
    }
    /*
     * 自定义函数：
     * 解析userConfig,累加日志groupList
     * 判断是否异常 	
     * 如异常则返回ArrayList[[apiId,indicatorId,strategyId,timestamp]]
     * 
     */
    public static ArrayList<ArrayList<String>> AlertOrNot(String userConfig,ArrayList<Map<String,Map<String, Map<String, Long>>>> groupList) {
        ArrayList<ArrayList<String>> abnormalList = new ArrayList<ArrayList<String>>();
        JSONArray userConfigJson = new JSONArray(userConfig);
        
        for(int m=0; m<userConfigJson.length(); m++) {
            JSONObject config = new JSONObject(userConfigJson.getString(m));
            Integer period = config.getInt("period");
            String indicatorId = config.getString("indicatorId");
            String strategyId = config.getString("strategyId");
            JSONArray apiIdList = config.getJSONArray("apiId");
            JSONArray content = config.getJSONArray("content");
            
            //遍历userConfig中的apiId
            for (int j=0; j<apiIdList.length(); j++) {	
                //聚合userConfig数据
                Map<String, Map<String, Long>> httpcodeMap = new HashMap<String, Map<String, Long>>();
                Map<String, Long> dataMap = new HashMap<String, Long>();
                ArrayList<String> httpcodeSet = new ArrayList<String>();
                    
                String apiId = apiIdList.getString(j);
                //获取httpcode全集
                for(int i=0; i<period/60; i++) {
                    //如果groupList有这个apiId则处理
                    if(groupList.get(i).containsKey(apiId)) {
                        httpcodeSet.addAll(groupList.get(i).get(apiId).keySet());
                    }
                }
                //聚合5/10分钟数据
                for(String httpcode : httpcodeSet) {
                    Long request = (long) 0;
                    Long sumElapse = (long) 0;
                    for(int k=0; k<period/60; k++) {
                        if(groupList.get(k).get(apiId).get(httpcode)!=null) {
                            request = groupList.get(k).get(apiId).get(httpcode).get("request") + 
                                request;	
                            sumElapse = groupList.get(k).get(apiId).get(httpcode).get("sumElapse") + 
                                sumElapse;
                        }
                    }
                    dataMap.put("request",request);
                    dataMap.put("sumElapse",sumElapse);
                    httpcodeMap.put(httpcode,dataMap);
                }
                //遍历报警条件,并判断
                int count = 0;
                int label = 0;
                for(int l = 0; l<content.length(); l++) {	
                    JSONObject cond = new JSONObject(content.getString(l));
                    count = count +1;
                    String compare = cond.getString("compare");
                    Float number = Float.parseFloat(cond.getString("number"));
                    //httpcode判断
                    if(cond.getString("indicatorSet") == "httpcode") {
                        String codeSet = cond.getString("codeSet");
                        if(compare == ">") {
                            if(httpcodeMap.get(codeSet).get("request") > number) {
                                label = label +1;
                            }
                        }
                        else if (compare == "<") {
                            if(httpcodeMap.get(codeSet).get("request") < number) {
                                label = label +1;
                            }
                        }
                    }
                    //request判断
                    else if(cond.get("indicatorSet") == "request") {
                        if(compare == ">") {
                            if(httpcodeMap.get("total").get("request") > number) {
                                label = label +1;
                            }
                        }
                        else if (compare == "<") {
                            if(httpcodeMap.get("total").get("request") < number) {
                                label = label +1;
                            }
                        }
                    }
                    //elapse判断
                    else if(cond.get("indicatorSet") == "elapse") {
                        if(compare == ">") {
                            if(httpcodeMap.get("total").get("sumElapse")/httpcodeMap.get("total").get("request") > number) {
                                label = label +1;
                            }	
                        }
                        else if (compare == "<") {
                            if(httpcodeMap.get("total").get("sumElapse")/httpcodeMap.get("total").get("request") < number) {
                                label = label +1;
                            }
                        }
                    }
                }
                if(label == count) {
                    String timestamp = String.valueOf(System.currentTimeMillis());
                    ArrayList<String> abnormal = new ArrayList<String>(Arrays.asList(apiId,indicatorId,strategyId,timestamp));
                    abnormalList.add(abnormal);
                }
            }	
        }
        return abnormalList;
    }
    @Override
    public void execute(Tuple input) {
        String userConfig = input.getStringByField("userConfig");
        ArrayList<Map<String,Map<String, Map<String, Long>>>> groupList = (ArrayList<Map<String,Map<String, Map<String, Long>>>>)input.getValueByField("groupList");
        if (this.isTickTuple(input)) {
            ArrayList<ArrayList<String>> abnormalList = (ArrayList<ArrayList<String>>)AlertOrNot(userConfig,groupList);
            collector.emit("abnormalList",new Values(abnormalList));
        }
    }	
    protected static boolean isTickTuple(Tuple tuple) {
        return tuple.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
            && tuple.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID);
    }
    /*
     * The bolt will emit the field minute,sum(elapse),httpcode,request 
     */
    @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declareStream("abnormalList",new Fields("abnormalList"));
        }
}
