package gov.epa.emissions.commons.io.other;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.importer.Parser;

public class InventoryTableParser implements Parser {

    private FileFormat fileFormat;

    public InventoryTableParser(FileFormat fileFormat) {
        this.fileFormat = fileFormat;
    }

    public Record parse(String line) {
        Record record = new Record();
        addTokens(line, record, fileFormat.cols());
            
        return record;
    }

    private void addTokens(String line, Record record, Column[] columns) {
        int offset = 0;
        int linelen = line.length();
        int mark = 0;
        int numcols = columns.length;
        int i;
        
        boolean endofline = false;
        String data = null;
        
        ForLoop:
        for (i = 0; i < numcols; i++) {
            mark = offset + columns[i].width();

            if (endofline){
                break ForLoop;
            }
            
            if ((i == numcols -1) || (mark > linelen)){
                data = line.substring(offset);
                endofline = true;
            }
            else {
                data = line.substring(offset, offset + columns[i].width());
            }           
            
            record.add(data);
            offset += columns[i].width();
        }
        
        //If line ends without specified column values, we need to put blank
        //string there
        while (i < numcols) { 
            record.add("");
            i++;
        }         
    }
    
}
