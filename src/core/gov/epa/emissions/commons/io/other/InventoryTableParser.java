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

//    private void addTokens(String line, Record record, Column[] columns) {
//        int offset = 0;
//        int linelen = line.length();
//        int mark = 0;
//        int numcols = columns.length;
//        int i;
//        
//        boolean endofline = false;
//        String data = null;
//        
//        ForLoop:
//        for (i = 0; i < numcols; i++) {
//            mark = offset + columns[i].width();
//
//            if (endofline){
//                break ForLoop;
//            }
//            
//            if ((i == numcols -1) || (mark > linelen)){
//                data = line.substring(offset);
//                endofline = true;
//            }
//            else {
//                data = line.substring(offset, offset + columns[i].width());
//            }           
//            
//            record.add(data);
//            offset += columns[i].width();
//        }
//        
//        //If line ends without specified column values, we need to put blank
//        //string there
//        while (i < numcols) { 
//            record.add("");
//            i++;
//        }         
//    }

    private void addTokens(String inLine, Record record, Column[] columns) {
        int numCols = columns.length, i;
        String[] bipart = splitLineByInlineComment(inLine);
        String left = bipart[0];
        
        for (i = 0; i < numCols; i++) {
            if (left.length() < columns[i].width()) {
                String data = left.substring(0);
                record.add(data);
                break;
            }

            String data = left.substring(0, columns[i].width());
            left = left.substring(columns[i].width());
            
            record.add(data);
        }
        
        // If line ends without specified column values, we need to put blank
        // string there
        while (i < numCols - 1) { 
            record.add("");
            i++;
        }

        if(bipart[1].length() > 0) // add inline comment if there is one
            record.add(bipart[1]);
    }
    
    private String[] splitLineByInlineComment(String line) {
        String[] bipart = {line, ""};
        int bang = line.indexOf('!');
        
        if(bang >= 0) {
            bipart[0] = line.substring(0, bang);
            bipart[1] = line.substring(bang);
        }
        
        return bipart;    
    }
    
}
