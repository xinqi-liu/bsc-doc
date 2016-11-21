import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.ShellBolt;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import monitorSpout.ApiAlertSpoutLog;
import monitorBolt.ApiAlertBoltConfig;
import monitorBolt.ApiAlertBoltLogParse;
import monitorBolt.ApiAlertBoltLogGroup;
import monitorBolt.ApiAlertBoltAbnormal;
import monitorBolt.ApiAlertBoltAlert;

/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class ApiAlertTopologyMain {

	public static void main(String[] args) throws Exception {

		// 实例化TopologyBuilder类
		TopologyBuilder builder = new TopologyBuilder();
		// 设置喷发节点并分配并发数，该并发数将会控制该对象在集群中的线程数
		builder.setSpout("ApiAlertSpoutLog", new ApiAlertSpoutLog(), 1);
		// 设置数据处理节点并分配并发数。指定该节点接收喷发节点的策略为随机方式。
		builder.setBolt("ApiAlertBoltConfig", new ApiAlertBoltConfig(), 1);

		builder.setBolt("ApiAlertBoltLogParse", new ApiAlertBoltLogParse(), 12)
			.shuffleGrouping("ApiAlertSpoutLog");
		
		builder.setBolt("ApiAlertBoltLogGroup", new ApiAlertBoltLogGroup(), 12)
			.fieldsGrouping("ApiAlertBoltLogParse","logParse", new Fields("apiId"));

		builder.setBolt("ApiAlertBoltAbnormal", new ApiAlertBoltAbnormal(), 12)
			.fieldsGrouping("ApiAlertBoltLogGroup","groupList", new Fields("groupList"))
			.fieldsGrouping("ApiAlertBoltConfig","userConfig", new Fields("userConfig"));

		builder.setBolt("ApiAlertBoltAlert", new ApiAlertBoltAlert(), 12)
			.fieldsGrouping("ApiAlertBoltAbnormal","abnormalList", new Fields("abnormalList"));
		
		Config conf = new Config();
		conf.setDebug(true);
		//conf.setNumWorkers(12);
		conf.setNumAckers(0);
		conf.put("wordsFile", args[1]) ;

		//if (args != null && args.length > 0) {
		if (false) {
	  		conf.setNumWorkers(1);
	  		StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
		}
		else {
	 		conf.setMaxTaskParallelism(1);
      LocalCluster cluster = new LocalCluster();
	 		cluster.submitTopology("ApiAlert", conf, builder.createTopology());
			Thread.sleep(30000) ;
      cluster.shutdown();
		}
  }
}	
