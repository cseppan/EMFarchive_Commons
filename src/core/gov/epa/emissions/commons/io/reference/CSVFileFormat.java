package gov.epa.emissions.commons.io.reference;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DelimitedFileFormat;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.List;

public class CSVFileFormat implements FileFormat, DelimitedFileFormat {

    private Column[] cols;

    private SqlDataTypes types;

    public CSVFileFormat(SqlDataTypes types, String[] colNames) {
        this.types = types;
        cols = createCols(colNames);
    }
    
    public CSVFileFormat(SqlDataTypes types, String[] colNames, String[] dataTypes) {
        this.types = types;
        cols = createCols(colNames, dataTypes);
    }

    private Column[] createCols(String[] colNames, String[] dataTypes) {
        //parse the header if it has a #COLUMN_TYPES to create columns of appropriate types
        List<Column> cols = new ArrayList<Column>();
        
        for (int i = 0; i < colNames.length; i++) {
            String name = replaceSpecialChars(colNames[i]);
            //System.out.println("column type   " + i + " " + dataTypes[i]); 
            if (dataTypes[i].toUpperCase().startsWith("VARCHAR")){
                int startIndex = dataTypes[i].lastIndexOf("(");
                int endIndex = dataTypes[i].lastIndexOf(")");
                //System.out.println(dataTypes[i]+ ": startIndes "+ startIndex + " endIndex " + endIndex);
                int charLen = Integer.parseInt(dataTypes[i].substring(++startIndex, endIndex));
                //System.out.println("charlen is  " + charLen); 
                if (charLen == 0)
                    cols.add(new Column(name, types.stringType(255), 255, new StringFormatter(255)));
                else
                    cols.add(new Column(name, types.stringType(charLen), charLen, new StringFormatter(charLen)));
            }
            else if (dataTypes[i].toUpperCase().startsWith("TEXT"))
                cols.add(new Column(name, types.stringType(255), 255, new StringFormatter(255)));
            else if (dataTypes[i].toUpperCase().startsWith("INT"))
                cols.add(new Column(name, types.intType(), new IntegerFormatter()));
            else if (dataTypes[i].toUpperCase().startsWith("REAL"))
                cols.add(new Column(name, types.realType(), new RealFormatter()));
            else if (dataTypes[i].toUpperCase().startsWith("FLOAT"))
                cols.add(new Column(name, types.realType(), new RealFormatter()));
            else if (dataTypes[i].toUpperCase().startsWith("BOOL"))
                cols.add(new Column(name, types.booleanType(), new StringFormatter(5)));
            else if (dataTypes[i].toUpperCase().startsWith("TIME"))
                cols.add(new Column(name, types.timestamp()));
            else 
                cols.add(new Column(name, types.stringType(255), 255, new StringFormatter(255)));
        }
        return cols.toArray(new Column[0]);
    }

    private Column[] createCols(String[] colNames) {
        List cols = new ArrayList();
        for (int i = 0; i < colNames.length; i++) {
            String name = replaceSpecialChars(colNames[i]);
            Column col = new Column(name, types.stringType(255), 255, new StringFormatter(255));
            cols.add(col);
        }
        return (Column[]) cols.toArray(new Column[] {});
    }

    private String replaceSpecialChars(String colName) {
        return colName.replace(' ', '_');
    }

    public String identify() {
        return "CSV File";
    }

    public Column[] cols() {
        return cols;
    }

}
