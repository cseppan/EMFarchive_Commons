package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class IDAHeaderReader {

	private BufferedReader reader;

	private String pollutantTag;

	private String[] pollutants;

	private List comments;

	public IDAHeaderReader(BufferedReader reader) {
		this.reader = reader;
		pollutantTag = "#DATA";
		comments = new ArrayList();
	}

	public String[] polluntants() {
		return pollutants;
	}
	
	public void read() throws ImporterException {
		String line = "";
		try {
			line = reader.readLine();
			while (line != null) {
				if (isComment(line)) {
					comments.add(line);
					if (isPollutantLine(line)) {
						pollutants = parse(line
								.substring(pollutantTag.length()));
						return;
					}
				}
				line = reader.readLine();
			}
			// FIXME: no header found
		} catch (IOException e) {
			throw new ImporterException("Could not read the header: " + line);
		}
	}

	private boolean isComment(String line) {
		line = line.trim();
		return line.length() != 0 && line.startsWith("#");
	}

	private String[] parse(String line) {
		Pattern pattern = Pattern.compile("\\s");
		return pattern.split(line.trim());
	}

	private boolean isPollutantLine(String line) {
		return (line.startsWith(pollutantTag));
	}

	public List comments() {
		return comments;

	}

}
