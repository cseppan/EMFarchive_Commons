package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FillDefaultValues;

import java.util.ArrayList;
import java.util.List;

public class IDAActivityFileFormat implements IDAFileFormat, FileFormatWithOptionalCols {

    private List requiredCols;

    private SqlDataTypes sqlDataTypes;

    private List optionalCols;

    private FillDefaultValues filler;

    public IDAActivityFileFormat(SqlDataTypes types, FillDefaultValues filler) {
        requiredCols = createRequiredCols(types);
        sqlDataTypes = types;
        this.filler= filler;
    }

    public void addPollutantCols(String[] pollutants) {
        optionalCols = pollutantCols(pollutants, sqlDataTypes);
    }

    public String identify() {
        return "IDA Activity";
    }

    public Column[] cols() {
        List allCols = new ArrayList();
        allCols.addAll(requiredCols);
        allCols.addAll(optionalCols);
        return (Column[])allCols.toArray(new Column[0]);
    }

    private List createRequiredCols(SqlDataTypes types) {
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

    public Column[] optionalCols() {
        return (Column[]) optionalCols.toArray(new Column[0]);
    }

    public Column[] minCols() {
        return (Column[]) requiredCols.toArray(new Column[0]);
    }

    public void fillDefaults(List data, long datasetId) {
        filler.fill(this, data, datasetId);        
    }
}
