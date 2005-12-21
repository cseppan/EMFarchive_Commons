package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.List;

public class IDAActivityFileFormat implements IDAFileFormat {

    private List cols;

    private SqlDataTypes sqlDataTypes;

    public IDAActivityFileFormat(SqlDataTypes types) {
        cols = createCols(types);
        sqlDataTypes = types;
    }

    public void addPollutantCols(String[] pollutants) {
        cols.addAll(pollutantCols(pollutants, sqlDataTypes));
    }

    public String identify() {
        return "IDA Activity";
    }

    public Column[] cols() {
        return (Column[]) cols.toArray(new Column[0]);
    }

    private List createCols(SqlDataTypes types) {
        List cols = new ArrayList();

        cols.add(new Column("STID", types.intType(), new IntegerFormatter()));
        cols.add(new Column("CYID", types.intType(), new IntegerFormatter()));
        cols.add(new Column("LINK_ID", types.stringType(10), new StringFormatter(10)));
        cols.add(new Column("SCC", types.stringType(10), new StringFormatter(10)));
        return cols;
    }

    private List pollutantCols(String[] pollutants, SqlDataTypes types) {
        List cols = new ArrayList();
        for (int i = 0; i < pollutants.length; i++) {
            Column col = new Column(pollutants[i], types.realType(), new RealFormatter());
            cols.add(col);
        }
        return cols;
    }
}
