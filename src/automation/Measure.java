package automation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import fim.Apriori;

public class Measure {

	private static final Log LOG = LogFactory.getLog(Measure.class);

	private final String id;
	private final String inputDir;
	private final String outputDir;
	private final long inputSize; // the number of baskets
	private final float minsup;

	private long elapsedTime;

	private double getElapsedSeconds() {
		// elaspedTime is in nanoseconds
		return elapsedTime / 1000000000.0;
	}

	public String getCSVreportLine() {
		final List<String> result = new ArrayList<String>();

		result.add(id);
		result.add(inputDir);
		result.add(String.valueOf(inputSize));
		result.add(String.valueOf(minsup));
		result.add(String.format("%.3f", getElapsedSeconds()));

		return StringUtils.join(",", result);
	}

	public Measure(final String csvLine) {
		LOG.info("Initalizing new measure with this config: " + csvLine);
		final String[] data = csvLine.trim().split(",|;");
		id = data[0];
		inputDir = data[1];
		outputDir = data[2];
		inputSize = Long.valueOf(data[3]);
		minsup = Float.valueOf(data[4]);
	}

	public void run() throws Exception {
		final Configuration conf = new Configuration();
		conf.setFloat("apriori.minsup", minsup);
		conf.setLong("apriori.baskets", inputSize);

		final String[] args = new String[] { inputDir, outputDir };

		final long startTime = System.nanoTime();
		ToolRunner.run(conf, new Apriori(), args);
		elapsedTime = System.nanoTime() - startTime;
	}
}
