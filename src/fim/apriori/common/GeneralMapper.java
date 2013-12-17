package fim.apriori.common;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

import automation.ProfileLogWriter;
import automation.ProfileLogWriter.TaskType;

public class GeneralMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private static final Log LOG = LogFactory.getLog(GeneralMapper.class);

	// We use this pattern a lot, pre-compile it for faster string splits
	private final Pattern space = Pattern.compile(" ");
	private CandidateTrie<String> candidateTrie;
	private int iteration;
	private boolean profile;

	private long candidateTrieBuldingTime;
	private long mapTime;
	private long mapStartTime;
	private long cleanupTime;
	private long completeMapperTime;
	private long fullMapperStartTime;

	private final Text text = new Text();
	private final IntWritable intw = new IntWritable();

	@Override
	public void setup(final Context context) throws IOException {
		fullMapperStartTime = System.nanoTime();
		iteration = context.getConfiguration().getInt("apriori.iteration", -1);
		profile = context.getConfiguration().getBoolean("measure.profile", false);

		LOG.info("Starting mapper. Iteration: " + iteration);

		BufferedReader reader = null;
		candidateTrie = new CandidateTrie<String>();
		final Path[] localCacheFiles = DistributedCache.getLocalCacheFiles(context.getConfiguration());
		if (localCacheFiles.length < 1) {
			throw new IOException("Could not read candidates file");
		}
		try {
			reader = new BufferedReader(new FileReader(localCacheFiles[0].toString()));
			String currentLine;
			while ((currentLine = reader.readLine()) != null) {
				candidateTrie.insert(space.split(currentLine.trim()), 0);
			}
		} catch (final FileNotFoundException e) {
			System.err.println("Could not open the candidates file");
			throw e;
		} catch (final IOException e) {
			System.err.println("Error during reading candidates file");
			throw e;
		} finally {
			if (reader != null) {
				reader.close();
			}
		}

		candidateTrieBuldingTime = System.nanoTime() - fullMapperStartTime;
		mapStartTime = System.nanoTime();
	}

	@Override
	public void map(final LongWritable key, final Text line, final Context context) throws IOException, InterruptedException {
		final String[] items = space.split(line.toString().trim());

		incrementCount(candidateTrie, Arrays.copyOfRange(items, 1, items.length), iteration);
		context.progress();

	}

	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException {
		mapTime = System.nanoTime() - mapStartTime;
		final long startTime = System.nanoTime();

		// Write the candidate trie to the output
		traverse(new ArrayList<String>(), candidateTrie, context);

		cleanupTime = System.nanoTime() - startTime;

		completeMapperTime = System.nanoTime() - fullMapperStartTime;
		if (profile) {
			writeProfileLogs(context);
		}
	}

	public void traverse(final List<String> path, final CandidateTrie<String> node, final Context context) throws IOException, InterruptedException {
		if (node.isLeaf()) {
			text.set(StringUtils.join(" ", path));
			intw.set(node.getCount());
			context.write(text, intw);
		} else {
			for (final String child : node.getChildren()) {
				final List<String> newPath = new ArrayList<String>(path);
				newPath.add(child);
				traverse(newPath, node.get(child), context);
			}
		}
	}

	public void incrementCount(final CandidateTrie<String> trie, final String[] items, final int depth) {
		if (items.length < depth) {
			return;
		}

		if (depth > 1) {
			for (int i = 0; i < items.length; i++) {
				if (trie.contains(items[i])) {
					incrementCount(trie.get(items[i]), Arrays.copyOfRange(items, i + 1, items.length), depth - 1);
				}
			}
		} else {
			for (final String item : items) {
				if (trie.contains(item)) {
					trie.get(item).increment();
				}
			}
		}
	}

	private void writeProfileLogs(final Context context) {
		final ProfileLogWriter logwriter = new ProfileLogWriter(context.getConfiguration(), TaskType.MAPPER);
		logwriter.addProperty("mapping time", String.valueOf(mapTime));
		logwriter.addProperty("candidate trie building time", String.valueOf(candidateTrieBuldingTime));
		logwriter.addProperty("trie traverse time", String.valueOf(cleanupTime));
		logwriter.addProperty("complete mapper time", String.valueOf(completeMapperTime));
		logwriter.write();
	}
}
