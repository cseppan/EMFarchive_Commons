package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
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
						pollutants = parsePollutants(line);
						return;
					}
				}
				line = reader.readLine();
			}
			throw new ImporterException("Could not find pollutant tag either '"
					+ pollutantTag1 + "' or '" + pollutantTag2 + "'");
		} catch (IOException e) {
			throw new ImporterException("Could not read the header: " + line);
		}
	}

	private boolean isComment(String line) {
		line = line.trim();
		return line.length() != 0 && line.startsWith("#");
	}

	private String[] parsePollutants(String line) throws ImporterException {
		Pattern pattern = Pattern.compile("\\S+");
		Matcher matcher = pattern.matcher(line);
		List tokens = new ArrayList();
		matcher.find();//skip the pollutant tag
		while(matcher.find()){
			String token = line.substring(matcher.start(),matcher.end());
			tokens.add(token);
		}
		if(tokens.isEmpty())
			throw new ImporterException("No pollutants specified");
		return (String[])tokens.toArray(new String[0]);
	}

	private boolean isPollutantLine(String line) {
		return (line.startsWith(pollutantTag1) || (line
				.startsWith(pollutantTag2)));
	}

	public List comments() {
		return comments;

	}

}
