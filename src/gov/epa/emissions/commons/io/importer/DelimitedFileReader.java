package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.importer.temporal.PacketTerminator;
import gov.epa.emissions.commons.io.importer.temporal.Record;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

public class DelimitedFileReader {

	private File file;

	private String delimiter;

	private BufferedReader fileReader;

	public DelimitedFileReader(File file)
			throws FileNotFoundException {
		this.file = file;
		this.delimiter = findDelimiter();
		fileReader = new BufferedReader(new FileReader(file));
		
	}

	private String findDelimiter() {
		return " ";
	}

	public void close() throws IOException {
		fileReader.close();
		
	}

	public Record read() throws IOException {
		String line = fileReader.readLine();
        if (line==null)
            return new PacketTerminator();
        return doRead(line);
	}

	private Record doRead(String line) {
        Record record = new Record();
        String [] tokens = split(line);
        record.add(Arrays.asList(tokens));
        return record;
    }

	private String[] split(String line) {
		return line.split(delimiter);
	}
	

}
