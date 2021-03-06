package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FixedWidthFileFormat;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.List;

public class IDAMobileFileFormat implements IDAFileFormat, FixedWidthFileFormat {

    private List<Column> cols;

    private SqlDataTypes sqlDataTypes;

    public IDAMobileFileFormat(SqlDataTypes types) {
        cols = createCols(types);
        this.sqlDataTypes = types;
    }

    public void addPollutantCols(String[] pollutants) {
        cols.addAll(pollutantCols(pollutants, sqlDataTypes));

    }

    public String identify() {
        return "IDA Mobile";
    }

    public Column[] cols() {
        return cols.toArray(new Column[0]);
    }

    private List<Column> createCols(SqlDataTypes types) {
        List<Column> cols = new ArrayList<Column>();
        cols.add(new Column("STID", types.intType(), 2, new IntegerFormatter(2,0)));
        cols.add(new Column("CYID", types.intType(), 3, new IntegerFormatter(3,0)));
        cols.add(new Column("LINK_ID", types.stringType(10), 10, new StringFormatter(10)));
        cols.add(new Column("SCC", types.stringType(10), 10, new StringFormatter(10)));

        return cols;
    }

    private List<Column> pollutantCols(String[] pollutants, SqlDataTypes types) {
        List<Column> cols = new ArrayList<Column>();
        for (int i = 0; i < pollutants.length; i++) {
            cols.add(new Column(replaceSpecialChars(pollutants[i]), types.realType(), 10, new RealFormatter(10,0)));
            cols.add(new Column("AVD_" + pollutants[i], types.realType(), 10, new RealFormatter(10,0)));
        }
        return cols;
    }
    
    private String replaceSpecialChars(String colName) {
        if (Character.isDigit(colName.charAt(0)))
            colName = "_" + colName; 
        
        return colName.replace(' ', '_');
    }

}
