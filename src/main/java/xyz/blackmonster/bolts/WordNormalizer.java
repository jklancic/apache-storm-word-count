package xyz.blackmonster.bolts;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import xyz.blackmonster.spouts.WordReader;

/**
 * Splits the line into an array of words and emits each word separately.
 */
public class WordNormalizer extends BaseBasicBolt {

	public static final String ARG_NAME = "word";

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		// can also be retrieved by <code>input.getString(0)</code>
		// since there is only one value being passed
		String line = input.getStringByField(WordReader.ARG_NAME);
		String[] words = line.split(" ");

		for (String word : words) {
			word = word.trim();
			if (!word.isEmpty()) {
				word = word.toLowerCase();
				collector.emit(new Values(word));
			}
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(ARG_NAME));
	}
}
