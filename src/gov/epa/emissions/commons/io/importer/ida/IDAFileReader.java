package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.io.importer.ColumnsMetadata;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.Record;
import gov.epa.emissions.commons.io.importer.TerminatorRecord;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IDAFileReader implements Reader {

	private BufferedReader fileReader;

	private List comments;

	private ColumnsMetadata colsMetadata;

	public IDAFileReader(BufferedReader reader, ColumnsMetadata colsMetadata,
			List comments) {
		fileReader = reader;
		this.colsMetadata = colsMetadata;
		comments = new ArrayList();
		comments.addAll(comments);
	}

	public void close() throws IOException {
		fileReader.close();
	}

	public Record read() throws IOException {
		String line = fileReader.readLine();
		while (line != null) {
			if (isData(line))
				return doRead(line);
			if (isComment(line))
				comments.add(line);

			line = fileReader.readLine();
		}
		return new TerminatorRecord();
	}

	private boolean isData(String line) {
		return !(line.trim().length() == 0) && (!isComment(line));
	}

	private Record doRead(String line) {
		Record record = new Record();
		String[] tokens = tokenizer(line);
		record.add(Arrays.asList(tokens));

		return record;
	}

	private String[] tokenizer(String line) {
		int[] widths = colsMetadata.widths();
		int startIndex = 0;
		String[] tokens = new String[widths.length];
		for (int i = 0; i < widths.length; i++) {
			tokens[i] = line.substring(startIndex, startIndex + widths[i]);
			startIndex = startIndex + widths[i];
		}
		return tokens;
	}

	private boolean isComment(String line) {
		return line.startsWith("#");
	}

	public List comments() {
		return comments;
	}

}
