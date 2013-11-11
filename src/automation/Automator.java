package automation;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.mapred.FileAlreadyExistsException;

public class Automator {

	private static final Log LOG = LogFactory.getLog(Automator.class);

	private final List<Measure> measures;

	public Automator(final String configFilePath) throws IOException {
		measures = new ArrayList<Measure>();

		final BufferedReader reader = new BufferedReader(new FileReader(configFilePath));

		reader.readLine(); // throw away the header (first line)

		String currentLine;
		while ((currentLine = reader.readLine()) != null) {
			measures.add(new Measure(currentLine));
		}
		reader.close();
	}

	public void start() {
		for (final Measure measure : measures) {
			measure.run();
		}
	}

	private void generateReport() throws IOException {
		final Date date = new Date();
		final DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HHmm");

		final String fileName = "results-" + dateFormat.format(date) + ".csv";

		final File file = new File(fileName);

		if (!file.exists()) {
			if (!file.createNewFile()) {
				throw new FileAlreadyExistsException();
			}
		}

		final BufferedWriter writer = new BufferedWriter(new FileWriter(file.getAbsoluteFile()));

		for (final Measure measure : measures) {
			writer.write(measure.getCSVreportLine() + '\n');
		}

		writer.close();
	}

	public static void main(final String[] args) {
		final String fileName;
		if (args.length < 1) {
			LOG.warn("The first argument must be the config file name. Trying to use config.csv");
			fileName = "config.csv";
		} else {
			fileName = args[0];
		}
		try {
			final Automator automator = new Automator(fileName);
			automator.start();
			automator.generateReport();
		} catch (final FileNotFoundException e) {
			LOG.error("File " + fileName + " not found");
		} catch (final IOException e) {
			LOG.error("IO Exception" + e);
		}
	}
}
