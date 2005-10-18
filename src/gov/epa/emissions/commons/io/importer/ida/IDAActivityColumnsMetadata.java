package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IDAActivityColumnsMetadata implements ColumnsMetadata {

    private Column[] cols;

    public IDAActivityColumnsMetadata(String[] pollutants, SqlDataTypes types) {
        cols = createCols(types, pollutants);
    }

    public int[] widths() {
        return null;
    }

    public String[] colTypes() {
        Column[] cols = cols();

        List sqlTypes = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            sqlTypes.add(cols[i].sqlType());
        }

        return (String[]) sqlTypes.toArray(new String[0]);
    }

    public String[] colNames() {
        Column[] cols = cols();

        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            names.add(cols[i].name());
        }

        return (String[]) names.toArray(new String[0]);
    }

    public String identify() {
        return "IDA Activity";
    }

    public Column[] cols() {
        return cols;
    }

    private Column[] createCols(SqlDataTypes types, String[] pollutants) {
        Column stid = new Column(types.intType(), new IntegerFormatter(), "STID");
        Column cyid = new Column(types.intType(), new IntegerFormatter(), "CYID");
        Column linkId = new Column(types.stringType(10), new StringFormatter(10), "LINK_ID");
        Column scc = new Column(types.stringType(10), new StringFormatter(10), "SCC");

        List cols = new ArrayList();
        cols.addAll(Arrays.asList(new Column[] { stid, cyid, linkId, scc }));

        for (int i = 0; i < pollutants.length; i++) {
            Column col = new Column(types.realType(), new RealFormatter(), pollutants[i]);
            cols.add(col);
        }

        return (Column[]) cols.toArray(new Column[0]);
    }

}
