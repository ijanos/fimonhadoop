package fim;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class AprioriMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final Pattern space = Pattern.compile(" ");
	private boolean firstRun;
	private CandidateTrie<String> candidateTrie;
	private int iteration;

	@Override
	public void setup(final Context context) {
		iteration = context.getConfiguration().getInt("apriori.iteration", -1);
		switch (iteration) {
		case -1:
			System.err.println("Cannot get apriori.iteration");
			break;
		case 1:
			firstRun = true;
			break;
		default:
			firstRun = false;
		}

		if (firstRun) {
			return;
		}

		BufferedReader reader;
		String currentLine;
		candidateTrie = new CandidateTrie<String>();

		try {
			reader = new BufferedReader(new FileReader(Apriori.candidatesFile));
			while ((currentLine = reader.readLine()) != null) {
				candidateTrie.insert(space.split(currentLine.trim()), 0);
			}
		} catch (final FileNotFoundException e) {
			System.err.println("Could not open the candidates file");
			e.printStackTrace();
		} catch (final IOException e) {
			System.err.println("Error during reading candidates file");
			e.printStackTrace();
		}
	}

	@Override
	public void map(final LongWritable key, final Text line, final Context context) throws IOException, InterruptedException {
		final String[] items = space.split(line.toString().trim());
		if (firstRun) {
			// i starts from 1 because the first element is the basket id
			for (int i = 1; i < items.length; i++) {
				context.write(new Text(items[i]), new IntWritable(1));
			}
		} else {
			incrementCount(candidateTrie, Arrays.copyOfRange(items, 1, items.length - 1), iteration);
			context.progress();
		}
	}

	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException {
		// Write the candidate trie to the output
		traverse(new ArrayList<String>(), candidateTrie, context);
	}

	public void traverse(final List<String> path, final CandidateTrie<String> node, final Context context) throws IOException, InterruptedException {
		if (node.isLeaf()) {
			context.write(new Text(StringUtils.join(" ", path)), new IntWritable(node.getCount()));
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
					incrementCount(trie.get(items[i]), Arrays.copyOfRange(items, i + 1, items.length - 1), depth - 1);
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
}
