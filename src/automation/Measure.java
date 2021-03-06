package automation;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.ToolRunner;

import fim.apriori.singlereduce.Apriori;

public class Measure {

	private static final Log LOG = LogFactory.getLog(Measure.class);

	private final String id;
	private final String inputDir;
	private final String outputDir;
	private final long inputSize; // the number of baskets
	private final float minsup;
	private final String logPath;
	private boolean successful = true;

	private long elapsedTime;

	private double getElapsedSeconds() {
		// elaspedTime is in nanoseconds
		return elapsedTime / 1000000000.0;
	}

	public String getCSVreportLine() {
		if (!successful) {
			return id + ",error";
		}

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
		final DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmmss");
		id = data[0] + "_" + dateFormat.format(new Date());
		inputDir = data[1];
		outputDir = data[2];
		inputSize = Long.parseLong(data[3]);
		minsup = Float.valueOf(data[4]);
		logPath = data[5];
	}

	public void run() {
		final Configuration conf = new Configuration();
		conf.setFloat("apriori.minsup", minsup);
		conf.setLong("apriori.baskets", inputSize);
		conf.set("apriori.job.id", id);
		conf.set("apriori.profile.logpath", logPath);

		// Profile log generation is currently hardwired to be enabled
		conf.setBoolean("measure.profile", true);

		final String[] args = new String[] { inputDir, outputDir };

		final long startTime = System.nanoTime();
		final Apriori apriori = new Apriori();
		try {
			ToolRunner.run(conf, apriori, args);
		} catch (final Exception e) {
			LOG.error("Uncaught exception in Apriori:" + e);
			successful = false;
		}

		if (!apriori.isSuccessfull()) {
			successful = false;
		}

		elapsedTime = System.nanoTime() - startTime;
	}
}
