package storm.starter;

import java.util.Map;

import storm.starter.ExclamationTopology.ExclamationBolt;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.ISpoutOutputCollector;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.TestWordSpout;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class TimeTopology {
    static class TimeSpout implements IRichSpout {
        ISpoutOutputCollector _collector;
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this._collector = collector;
        }

        public void nextTuple() {
            long milliseconds = System.currentTimeMillis();
            _collector.emit("default", new Values(new Long(milliseconds)), new Long(milliseconds));
        }

        public Map<String, Object> getComponentConfiguration() {
            return null;
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("time"));
        }

        public void activate() {
  
        }

        public void deactivate() {

        }

        public void close() {
            // TODO Auto-generated method stub
            
        }

        public void ack(Object msgId) {
            // TODO Auto-generated method stub
            
        }

        public void fail(Object msgId) {
            // TODO Auto-generated method stub
            
        }
    }

    static class TimeBolt extends BaseBasicBolt {
        private OutputCollector _collector;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            this._collector = collector;
        }

        public void execute(Tuple tuple) {
            this._collector.emit(new Values(tuple.getLong(0) + 10000));
            this._collector.ack(tuple);
        }

        public void execute(Tuple input, BasicOutputCollector collector) {
            // TODO Auto-generated method stub
            
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("time"));
        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("time-spout", new TimeSpout(), 1);        
        builder.setBolt("time-bolt1", new TimeBolt(), 3)
                .shuffleGrouping("time-spout");
        builder.setBolt("time-bolt2", new TimeBolt(), 2)
            .shuffleGrouping("time-bolt1");
                
        Config conf = new Config();
        conf.setDebug(true);
        
        if(args!=null && args.length > 0) {
            conf.setNumWorkers(3);
            
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
        
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();    
        }
    }
}