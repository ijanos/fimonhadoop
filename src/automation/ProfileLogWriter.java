package automation;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class ProfileLogWriter {
	private static final Log LOG = LogFactory.getLog(ProfileLogWriter.class);

	private final HashMap<String, String> properties;
	private final Configuration conf;
	private final String aprioriJobID;
	private FileSystem fs;
	private FSDataOutputStream outputstream;
	private final TaskType taskType;
	private final String logpath;
	private final int iteration;

	public static enum TaskType {
		MAPPER, REDUCER, MRJOB
	}

	public ProfileLogWriter(final Configuration conf, final TaskType type) {
		properties = new HashMap<String, String>();
		this.conf = conf;
		this.taskType = type;
		aprioriJobID = conf.get("apriori.job.id");
		logpath = conf.get("apriori.profile.logpath");
		iteration = conf.getInt("apriori.iteration", -1);
		initHDFS();
	}

	public void addProperty(final String name, final String value) {
		properties.put(name, value);
	}

	public void write() {
		for (final String key : properties.keySet()) {
			final String line = aprioriJobID + ";" + iteration + ";" + key + "=" + properties.get(key) + "\n";
			try {
				outputstream.write(line.getBytes("UTF-8"));
			} catch (final IOException e) {
				LOG.error(e);
			}
		}
		try {
			outputstream.close();
		} catch (final IOException e) {
			LOG.error(e);
		}
	}

	private void initHDFS() {
		String hostname;
		try {
			hostname = java.net.InetAddress.getLocalHost().getHostName();
		} catch (final UnknownHostException e) {
			LOG.warn("Could not find hostname.");
			hostname = "UNKOWN_HOST";
		}

		final String postfix;

		switch (taskType) {
		case MAPPER:
			postfix = "iter" + iteration + "_" + hostname + "_mapper";
			break;
		case REDUCER:
			postfix = "iter" + iteration + "_" + hostname + "_reducer";
			break;
		case MRJOB:
			postfix = "jobcontroller_" + hostname;
			break;
		default:
			postfix = "unkown";
		}

		final String filename = aprioriJobID + "_" + postfix;
		final Path logFilePath = new Path(logpath + filename);
		try {
			fs = FileSystem.get(conf);
			outputstream = fs.create(logFilePath);
		} catch (final IOException e) {
			LOG.error("IO error while accessing Hadoop Filesystem.", e);
		}
	}
}
