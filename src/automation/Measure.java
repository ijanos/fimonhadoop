package automation;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.TaskCompletionEvent;
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
	private boolean successful = true;
	private List<TaskCompletionEvent[]> taskCompletionEvents;

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

	public void writeCompEvents() {
		for (final TaskCompletionEvent[] events : taskCompletionEvents) {
			System.out.println("===========");
			for (final TaskCompletionEvent event : events) {
				System.out.print("Id :" + event.toString() + " || " + event.getTaskAttemptId() + " || " + event.getTaskStatus() + " || " + event.getEventId());
				System.out.println(" || isMap: " + event.isMapTask() + " Taskrun time: " + event.getTaskRunTime());
			}
		}
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

	public void run() {
		final Configuration conf = new Configuration();
		Apriori apriori = null;
		conf.setFloat("apriori.minsup", minsup);
		conf.setLong("apriori.baskets", inputSize);

		final String[] args = new String[] { inputDir, outputDir };

		final long startTime = System.nanoTime();
		try {
			apriori = new Apriori();
			ToolRunner.run(conf, apriori, args);
			elapsedTime = System.nanoTime() - startTime;
			taskCompletionEvents = apriori.getTakCompletionEvents();
		} catch (final Exception e) {
			LOG.error("Uncaught exception in Apriori:" + e);
			successful = false;
		}

	}
}
