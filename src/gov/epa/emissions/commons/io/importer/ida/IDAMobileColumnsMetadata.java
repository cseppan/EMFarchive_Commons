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

public class IDAMobileColumnsMetadata implements ColumnsMetadata {

    private int[] widths;

    private Column[] cols;

    public IDAMobileColumnsMetadata(String[] pollutants, SqlDataTypes types) {
        cols = createCols(types, pollutants);

        int[] desColWidths = new int[] { 2, 3, 10, 10 };
        widths = addPollWidths(pollutants.length, desColWidths);
    }

    public int[] widths() {
        return widths;
    }

    private int[] addPollWidths(int length, int[] desColWidths) {
        int resolution = 2;
        int totalCols = desColWidths.length + resolution * length;
        int[] widths = new int[totalCols];
        for (int i = 0; i < desColWidths.length; i++) {
            widths[i] = desColWidths[i];
        }
        for (int i = 0; i < length; i++) {
            int startIndex = desColWidths.length + i * resolution;
            widths[startIndex] = 10;
            widths[startIndex + 1] = 10;
        }
        return widths;
    }

    public String identify() {
        return "IDA Mobile";
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
            Column ann = new Column(types.realType(), new RealFormatter(), "ANN_" + pollutants[i]);
            Column avd = new Column(types.realType(), new RealFormatter(), "AVD_" + pollutants[i]);

            cols.addAll(Arrays.asList(new Column[] { ann, avd }));
        }

        return (Column[]) cols.toArray(new Column[0]);
    }
}
