package fim;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class AprioriReducer extends Reducer<Text, IntWritable, Text, NullWritable> {

	private final Pattern space = Pattern.compile(" ");
	private int minsup;
	private int iteration;
	private List<String[]> largeItemsets;
	private List<String[]> candidateItems;

	@Override
	protected void setup(final Context context) throws IOException, InterruptedException {
		iteration = context.getConfiguration().getInt("apriori.iteration", -1);
		largeItemsets = new ArrayList<String[]>();
	}

	@Override
	protected void reduce(final Text itemset, final Iterable<IntWritable> counts, final Context context) throws IOException, InterruptedException {
		int sumCount = 0;
		for (final IntWritable count : counts) {
			sumCount += count.get();
		}
		if (sumCount > minsup) {
			largeItemsets.add(space.split(itemset.toString().trim()));
		}
		context.progress();
	}

	@Override
	protected void cleanup(final Context context) throws IOException, InterruptedException {
		// Generate k+1 candidate itemsets
		candidateItems = new ArrayList<String[]>();
		if (iteration == 1) {
			for (int i = 0; i < largeItemsets.size(); i++) {
				for (int j = i + 1; j < largeItemsets.size(); j++) {
					if (Integer.valueOf(largeItemsets.get(i)[0]) < Integer.valueOf(largeItemsets.get(j)[0])) {
						candidateItems.add(new String[] { largeItemsets.get(i)[0], largeItemsets.get(j)[0] });
					} else {
						candidateItems.add(new String[] { largeItemsets.get(j)[0], largeItemsets.get(i)[0] });
					}
				}
			}
		} else {
			for (int i = 0; i < largeItemsets.size(); i++) {
				for (int j = i + 1; j < largeItemsets.size(); j++) {
					final String[] itemset1 = largeItemsets.get(i);
					final String[] itemset2 = largeItemsets.get(j);
					if (compareArrays(itemset1, itemset2)) {
						final String[] itemset;
						final String item;
						if (Integer.valueOf(itemset1[itemset1.length - 1]) < Integer.valueOf(itemset2[itemset2.length - 1])) {
							itemset = itemset1;
							item = itemset2[itemset2.length - 1];
						} else {
							itemset = itemset2;
							item = itemset1[itemset1.length - 1];
						}
						final String[] candidate = Arrays.copyOf(itemset, itemset.length + 1);
						candidate[itemset.length] = item;
						candidateItems.add(candidate);
					}
				}
			}
		}
		if (candidateItems.isEmpty()) {
			context.getCounter(Apriori.FinishedCounter.FINISHED).increment(1);
		} else {
			for (final String[] candidate : candidateItems) {
				context.write(new Text(StringUtils.join(" ", candidate)), null);
			}
		}
	}

	/**
	 * 
	 * @param arr1
	 *            Array 1
	 * @param arr2
	 *            Array 2
	 * @return true if the the arrays first n-1 elements are the same where n is
	 *         the array length.
	 */
	private boolean compareArrays(final String[] arr1, final String[] arr2) {
		if (arr1.length != arr2.length) {
			System.err.println("Array size does not match. This should not happen!");
			return false;
		}
		boolean result = true;
		for (int i = 0; i < arr1.length - 1; i++) {
			if (!arr1[i].equals(arr2[i])) {
				result = false;
				break;
			}
		}
		return result;
	}

}
