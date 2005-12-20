package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.FileFormat;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Pattern;

public class SMKReportFileFormatFactory {

    private BufferedReader reader;
    
    private SqlDataTypes types;
    
    private String[] cols;
    
    private String delimiter;
    
    public SMKReportFileFormatFactory(File file, SqlDataTypes types) throws IOException {
        this.reader = new BufferedReader(new FileReader(file));
        this.types = types;
        this.delimiter = readHeader();
    }

    public FileFormat getFormat() throws Exception {
        return new SMKReportFileFormat(cols, types);
    }
    
    public String getDelimiter(){
        return delimiter;
    }
    
    private String readHeader() throws IOException {
        String comma = ",";
        String semicolon = ";";
        String pipe = "|";
        Pattern bar = Pattern.compile("[|]");
        String delimiter = null;
        
        for(String line = reader.readLine(); line != null; line = reader.readLine()) {
            if(line.split(comma).length >= 3) {
                delimiter = comma;
                cols = line.split(delimiter);
            }
            else if(line.split(semicolon).length >= 3) {
                delimiter = semicolon;
                cols = line.split(delimiter);
            }
            else if (bar.split(line).length >= 3) {
                delimiter = pipe;
                cols = bar.split(line);
            }
            
            if(delimiter != null) {
                reader.close();
                return delimiter;
            }
        }
            
        reader.close();
        return delimiter;
    }

}
