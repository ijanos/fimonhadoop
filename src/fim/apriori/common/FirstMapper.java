package fim.apriori.common;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import automation.ProfileLogWriter;
import automation.ProfileLogWriter.TaskType;

public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	private static final Log LOG = LogFactory.getLog(FirstMapper.class);

	private final Pattern space = Pattern.compile(" ");
	private boolean profile;

	private long mapTime;
	private long mapStartTime;

	@Override
	public void setup(final Context context) throws IOException {
		profile = context.getConfiguration().getBoolean("measure.profile", false);

		LOG.info("Starting mapper. First iteration.");

		mapStartTime = System.nanoTime();
	}

	@Override
	public void map(final LongWritable key, final Text line, final Context context) throws IOException, InterruptedException {
		final String[] items = space.split(line.toString().trim());

		// i starts from 1 because the first element is the basket id
		for (int i = 1; i < items.length; i++) {
			context.write(new Text(items[i]), new IntWritable(1));
		}

	}

	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException {
		mapTime = System.nanoTime() - mapStartTime;

		if (profile) {
			writeProfileLogs(context);
		}
	}

	private void writeProfileLogs(final Context context) {
		final ProfileLogWriter logwriter = new ProfileLogWriter(context.getConfiguration(), TaskType.MAPPER);
		logwriter.addProperty("Mapper time", String.valueOf(mapTime));
		logwriter.write();
	}
}