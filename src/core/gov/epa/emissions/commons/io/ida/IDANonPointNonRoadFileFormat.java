package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.List;

public class IDANonPointNonRoadFileFormat implements IDAFileFormat {

    private List cols;

    private SqlDataTypes sqlDataTypes;

    public IDANonPointNonRoadFileFormat( SqlDataTypes types) {
        cols = createCols(types);
        sqlDataTypes = types;
    }

    public void addPollutantCols(String[] pollutants) {
        cols.addAll(pollutantCols(pollutants, sqlDataTypes));
    }

    public String identify() {
        return "IDA Area";
    }

    public Column[] cols() {
        return (Column[]) cols.toArray(new Column[0]);
    }

    private List createCols(SqlDataTypes types) {
        List cols = new ArrayList();

        cols.add(new Column("STID", types.intType(), 2, new IntegerFormatter()));
        cols.add(new Column("CYID", types.intType(), 3, new IntegerFormatter()));
        cols.add(new Column("SCC", types.stringType(10), 10, new StringFormatter(10)));
        return cols;
    }

    private List pollutantCols(String[] pollutants, SqlDataTypes types) {
        List cols = new ArrayList();
        for (int i = 0; i < pollutants.length; i++) {
            cols.add(new Column("ANN_" + pollutants[i], types.realType(), 10, new RealFormatter()));
            cols.add(new Column("AVD_" + pollutants[i], types.realType(), 10, new RealFormatter()));
            cols.add(new Column("EMF_" + pollutants[i], types.realType(), 11, new RealFormatter()));
            cols.add(new Column("CE_" + pollutants[i], types.realType(), 7, new RealFormatter()));
            cols.add(new Column("RE_" + pollutants[i], types.realType(), 3, new RealFormatter()));
            cols.add(new Column("RP_" + pollutants[i], types.realType(), 6, new RealFormatter()));
        }
        return cols;
    }

}
