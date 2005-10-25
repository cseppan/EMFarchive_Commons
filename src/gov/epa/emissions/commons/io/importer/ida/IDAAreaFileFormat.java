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

public class IDAAreaFileFormat implements FileFormat {

    private Column[] cols;

    public IDAAreaFileFormat(String[] pollutants, SqlDataTypes types) {
        cols = createCols(types, pollutants);
    }

    public String identify() {
        return "IDA Area";
    }

    public Column[] cols() {
        return cols;
    }

    private Column[] createCols(SqlDataTypes types, String[] pollutants) {
        Column stid = new Column("STID", types.intType(), 2, new IntegerFormatter());
        Column cyid = new Column("CYID", types.intType(), 3, new IntegerFormatter());
        Column scc = new Column("SCC", types.stringType(10), 10, new StringFormatter(10));

        List cols = new ArrayList();
        cols.addAll(Arrays.asList(new Column[] { stid, cyid, scc }));

        for (int i = 0; i < pollutants.length; i++) {
            Column ann = new Column("ANN_" + pollutants[i], types.realType(), 10, new RealFormatter());
            Column avd = new Column("AVD_" + pollutants[i], types.realType(), 10, new RealFormatter());
            Column emf = new Column("EMF_" + pollutants[i], types.realType(), 11, new RealFormatter());
            Column ce = new Column("CE_" + pollutants[i], types.realType(), 7, new RealFormatter());
            Column re = new Column("RE_" + pollutants[i], types.realType(), 3, new RealFormatter());
            Column rp = new Column("RP_" + pollutants[i], types.realType(), 6, new RealFormatter());

            cols.addAll(Arrays.asList(new Column[] { ann, avd, emf, ce, re, rp }));
        }

        return (Column[]) cols.toArray(new Column[0]);
    }

}
