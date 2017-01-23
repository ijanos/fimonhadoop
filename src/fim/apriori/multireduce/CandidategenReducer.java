package fim.apriori.multireduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import automation.ProfileLogWriter;
import automation.ProfileLogWriter.TaskType;

public class CandidategenReducer extends Reducer<Text, Text, Text, NullWritable> {

	private static final Log LOG = LogFactory.getLog(CandidategenReducer.class);

	private boolean profile;
	private long reduceStart;
	private long candidateGenTime;

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		LOG.info("Starting reducer for candidate generation");

		profile = context.getConfiguration().getBoolean("measure.profile", false);

		reduceStart = System.nanoTime();
	}

	@Override
	protected void reduce(final Text itemset, final Iterable<Text> lastItems, final Context context) throws IOException, InterruptedException {

		final List<String> lastItemsList = new ArrayList<String>();
		for (final Text lastItem : lastItems) {
			lastItemsList.add(lastItem.toString());
		}

		if (lastItemsList.size() < 2) {
			return;
		}

		for (int i = 0; i < lastItemsList.size(); i++) {
			for (int j = i + 1; j < lastItemsList.size(); j++) {
				if (Integer.valueOf(lastItemsList.get(i)) < Integer.valueOf(lastItemsList.get(j))) {
					context.write(new Text(itemset.toString() + " " + lastItemsList.get(i) + " " + lastItemsList.get(j)), null);
				} else {
					context.write(new Text(itemset.toString() + " " + lastItemsList.get(j) + " " + lastItemsList.get(i)), null);
				}
			}
		}
		if (profile) {
			writeProfileLogs(context);
		}
	}

	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException {
		candidateGenTime = System.nanoTime() - reduceStart;
	}

	private void writeProfileLogs(final Context context) {
		final ProfileLogWriter logwriter = new ProfileLogWriter(context.getConfiguration(), TaskType.REDUCER);
		logwriter.addProperty("Candidate generation time", String.valueOf(candidateGenTime));
		logwriter.write();
	}
}
