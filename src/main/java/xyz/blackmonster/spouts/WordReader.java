package xyz.blackmonster.spouts;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

/**
 * Spout to read text file and stream words.
 */
public class WordReader extends BaseRichSpout {

	public static final String ARG_NAME = "line";

	private SpoutOutputCollector collector;
	private FileReader fileReader;
	private boolean completed = false;

	@Override
	public void ack(Object msgId) {
		System.out.println("SUCCESS: " + msgId);
	}

	@Override
	public void fail(Object msgId) {
		System.out.println("FAIL: " + msgId);
	}

	@Override
	public void close() {
		System.out.println("CLOSE");
	}

	@Override
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.fileReader = new FileReader(conf.get("wordsFile").toString());
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Error reading file " + conf.get("wordFile"));
		}
		this.collector = collector;
	}

	@Override
	public void nextTuple() {
		if (completed) {
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				System.out.println("Error: " + e.getMessage());
			}
			System.out.println("DONE");
			return;
		}

		String line;
		BufferedReader reader = new BufferedReader(fileReader);
		try {
			while ((line = reader.readLine()) != null) {
				this.collector.emit(new Values(line), line);
			}
		} catch (Exception e) {
			throw new RuntimeException("Error reading tuple", e);
		} finally {
			completed = true;
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(ARG_NAME));
	}
}
