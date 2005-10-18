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

public class IDAAreaColumnsMetadata implements ColumnsMetadata {

    private int[] widths;

    private Column[] cols;

    public IDAAreaColumnsMetadata(String[] pollutants, SqlDataTypes types) {
        cols = createCols(types, pollutants);

        int[] desColWidths = new int[] { 2, 3, 10 };
        widths = addPollWidths(pollutants.length, desColWidths);
    }

    public int[] widths() {
        return widths;
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

    private int[] addPollWidths(int length, int[] desColWidths) {
        int resolution = 6;
        int totalCols = desColWidths.length + resolution * length;
        int[] widths = new int[totalCols];
        for (int i = 0; i < desColWidths.length; i++) {
            widths[i] = desColWidths[i];
        }
        for (int i = 0; i < length; i++) {
            int startIndex = desColWidths.length + i * resolution;
            widths[startIndex] = 10;
            widths[startIndex + 1] = 10;
            widths[startIndex + 2] = 11;
            widths[startIndex + 3] = 7;
            widths[startIndex + 4] = 3;
            widths[startIndex + 5] = 6;
        }
        return widths;
    }

    public String identify() {
        return "IDA Area";
    }

    public Column[] cols() {
        return cols;
    }

    private Column[] createCols(SqlDataTypes types, String[] pollutants) {
        Column stid = new Column(types.intType(), new IntegerFormatter(), "STID");
        Column cyid = new Column(types.intType(), new IntegerFormatter(), "CYID");
        Column scc = new Column(types.stringType(10), new StringFormatter(10), "SCC");

        List cols = new ArrayList();
        cols.addAll(Arrays.asList(new Column[] { stid, cyid, scc }));

        for (int i = 0; i < pollutants.length; i++) {
            Column ann = new Column(types.realType(), new RealFormatter(), "ANN_" + pollutants[i]);
            Column avd = new Column(types.realType(), new RealFormatter(), "AVD_" + pollutants[i]);
            Column emf = new Column(types.realType(), new RealFormatter(), "EMF_" + pollutants[i]);
            Column ce = new Column(types.realType(), new RealFormatter(), "CE_" + pollutants[i]);
            Column re = new Column(types.realType(), new RealFormatter(), "RE_" + pollutants[i]);
            Column rp = new Column(types.realType(), new RealFormatter(), "RP_" + pollutants[i]);

            cols.addAll(Arrays.asList(new Column[] { ann, avd, emf, ce, re, rp }));
        }

        return (Column[]) cols.toArray(new Column[0]);
    }

}
