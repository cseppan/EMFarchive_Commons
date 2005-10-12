package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class IDAHeaderReader {

	private BufferedReader reader;

	private String pollutantTag1;

	private String pollutantTag2;
	
	private String[] pollutants;

	private List comments;

	

	public IDAHeaderReader(BufferedReader reader) {
		this.reader = reader;
		pollutantTag1 = "#DATA";
		pollutantTag2 = "#POLID";
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
						pollutants = parse(line);
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
		if(line.startsWith(pollutantTag1)){
			line = line.substring(pollutantTag1.length());
		}
		else{
			line = line.substring(pollutantTag2.length());
		}
		return pattern.split(line.trim());
	}

	private boolean isPollutantLine(String line) {
		return (line.startsWith(pollutantTag1));
	}

	public List comments() {
		return comments;

	}

}
