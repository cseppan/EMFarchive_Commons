package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FileFormat;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class IDAMobileFileFormat implements FileFormat {

    private Column[] cols;

    public IDAMobileFileFormat(String[] pollutants, SqlDataTypes types) {
        cols = createCols(types, pollutants);
    }

    public String identify() {
        return "IDA Mobile";
    }

    public Column[] cols() {
        return cols;
    }

    private Column[] createCols(SqlDataTypes types, String[] pollutants) {
        Column stid = new Column("STID", types.intType(), 2, new IntegerFormatter());
        Column cyid = new Column("CYID", types.intType(), 3, new IntegerFormatter());
        Column linkId = new Column("LINK_ID", types.stringType(10), 10, new StringFormatter(10));
        Column scc = new Column("SCC", types.stringType(10), 10, new StringFormatter(10));

        List cols = new ArrayList();
        cols.addAll(Arrays.asList(new Column[] { stid, cyid, linkId, scc }));

        for (int i = 0; i < pollutants.length; i++) {
            Column ann = new Column("ANN_" + pollutants[i], types.realType(), 10, new RealFormatter());
            Column avd = new Column("AVD_" + pollutants[i], types.realType(), 10, new RealFormatter());

            cols.addAll(Arrays.asList(new Column[] { ann, avd }));
        }

        return (Column[]) cols.toArray(new Column[0]);
    }
}
