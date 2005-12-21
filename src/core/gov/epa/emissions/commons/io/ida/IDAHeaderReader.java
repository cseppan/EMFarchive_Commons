package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
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

    public IDAHeaderReader(File file) throws ImporterException {
        try {
            this.reader = new BufferedReader(new FileReader(file));
        } catch (FileNotFoundException e) {
            throw new ImporterException("File not found -"+e.getMessage());
        }
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
                line = line.trim();
                if (isComment(line))
                    processComments(line);
                if (isData(line))
                    break;
                line = reader.readLine();
            }
            pollutantFound();

        } catch (IOException e) {
            throw new ImporterException("Could not read the header: " + line);
        }
    }

    private void processComments(String line) throws ImporterException {
        comments.add(line);
        if (isPollutantLine(line)) {
            pollutants = parsePollutants(line);
        }
    }

    private void pollutantFound() throws ImporterException {
        if (pollutants == null || pollutants.length == 0)
            throw new ImporterException("Could not find pollutant tag either '" + pollutantTag1 + "' or '"
                    + pollutantTag2 + "'");

    }

    private boolean isComment(String line) {
        return line.length() != 0 && line.startsWith("#");
    }

    private boolean isData(String line) {
        return line.length() != 0 && !line.startsWith("#");
    }

    private String[] parsePollutants(String line) throws ImporterException {
        Pattern pattern = Pattern.compile("\\S+");
        Matcher matcher = pattern.matcher(line);
        List tokens = new ArrayList();
        matcher.find();// skip the pollutant tag
        while (matcher.find()) {
            String token = line.substring(matcher.start(), matcher.end());
            tokens.add(token);
        }
        if (tokens.isEmpty())
            throw new ImporterException("No pollutants specified");
        return (String[]) tokens.toArray(new String[0]);
    }

    private boolean isPollutantLine(String line) {
        return (line.startsWith(pollutantTag1) || (line.startsWith(pollutantTag2)));
    }

    public List comments() {
        return comments;

    }

    public void close() throws ImporterException {
        try {
            reader.close();
        } catch (IOException e) {
           throw new ImporterException("Error in closing IDA header reader",e);
        }
    }

}
