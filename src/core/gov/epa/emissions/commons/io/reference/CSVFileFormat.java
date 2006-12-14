package gov.epa.emissions.commons.io.reference;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.DelimitedFileFormat;
import gov.epa.emissions.commons.io.FileFormat;
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
