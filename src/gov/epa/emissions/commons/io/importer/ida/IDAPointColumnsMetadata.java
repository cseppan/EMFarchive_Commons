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

public class IDAPointColumnsMetadata implements ColumnsMetadata {

    private int[] widths;

    private Column[] cols;

    public IDAPointColumnsMetadata(String[] pollutants, SqlDataTypes types) {
        cols = createCols(types, pollutants);

        int[] desColWidths = new int[] { 2, 3, 15, 15, 12, 6, 6, 2, 40, 10, 4, 4, 4, 6, 4, 10, 9, 8, 1, 2, 2, 2, 2, 2,
                2, 1, 2, 11, 12, 8, 5, 5, 9, 4, 9, 9, 1 };
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
        int resolution = 7;
        int totalCols = desColWidths.length + resolution * length;
        int[] widths = new int[totalCols];
        for (int i = 0; i < desColWidths.length; i++) {
            widths[i] = desColWidths[i];
        }
        for (int i = 0; i < length; i++) {
            int startIndex = desColWidths.length + i * resolution;
            widths[startIndex] = 13;
            widths[startIndex + 1] = 13;
            widths[startIndex + 2] = 7;
            widths[startIndex + 3] = 3;
            widths[startIndex + 4] = 10;
            widths[startIndex + 5] = 3;
            widths[startIndex + 6] = 3;
        }
        return widths;
    }

    public String identify() {
        return "IDA Point";
    }

    public Column[] cols() {
        return cols;
    }

    private Column[] createCols(SqlDataTypes types, String[] pollutants) {
        List cols = new ArrayList();

        Column[] mandatory = createMandatoryCols(types);
        cols.addAll(Arrays.asList(mandatory));
        addPollutantsBasedCols(types, pollutants, cols);

        return (Column[]) cols.toArray(new Column[0]);
    }

    private void addPollutantsBasedCols(SqlDataTypes types, String[] pollutants, List cols) {
        for (int i = 0; i < pollutants.length; i++) {
            Column ann = new Column(types.realType(), new RealFormatter(), "ANN_" + pollutants[i]);
            Column avd = new Column(types.realType(), new RealFormatter(), "AVD_" + pollutants[i]);
            Column ce = new Column(types.realType(), new RealFormatter(), "CE_" + pollutants[i]);
            Column re = new Column(types.realType(), new RealFormatter(), "RE_" + pollutants[i]);
            Column emf = new Column(types.realType(), new RealFormatter(), "EMF_" + pollutants[i]);
            Column cpri = new Column(types.realType(), new RealFormatter(), "CPRI_" + pollutants[i]);
            Column csec = new Column(types.realType(), new RealFormatter(), "CSEC_" + pollutants[i]);

            cols.addAll(Arrays.asList(new Column[] { ann, avd, ce, re, emf, cpri, csec }));
        }
    }

    private Column[] createMandatoryCols(SqlDataTypes types) {
        List cols = new ArrayList();

        cols.add(new Column(types.intType(), new IntegerFormatter(), "STID"));
        cols.add(new Column(types.intType(), new IntegerFormatter(), "CYID"));
        cols.add(new Column(types.stringType(15), new StringFormatter(15), "PLANTID"));
        cols.add(new Column(types.stringType(15), new StringFormatter(15), "POINTID"));
        cols.add(new Column(types.stringType(12), new StringFormatter(12), "STACKID"));
        cols.add(new Column(types.stringType(6), new StringFormatter(6), "ORISID"));
        cols.add(new Column(types.stringType(6), new StringFormatter(6), "BLRID"));
        cols.add(new Column(types.stringType(2), new StringFormatter(2), "SEGMENT"));
        cols.add(new Column(types.stringType(40), new StringFormatter(40), "PLANT"));
        cols.add(new Column(types.stringType(10), new StringFormatter(10), "SCC"));
        cols.add(new Column(types.intType(), new IntegerFormatter(), "BEGYR"));
        cols.add(new Column(types.intType(), new IntegerFormatter(), "ENDYR"));
        cols.add(new Column(types.realType(), new RealFormatter(), "STKHGT"));
        cols.add(new Column(types.realType(), new RealFormatter(), "STKDIAM"));
        cols.add(new Column(types.realType(), new RealFormatter(), "STKTEMP"));
        cols.add(new Column(types.realType(), new RealFormatter(), "STKFLOW"));
        cols.add(new Column(types.realType(), new RealFormatter(), "STKVEL"));
        cols.add(new Column(types.realType(), new RealFormatter(), "BOILCAP"));
        cols.add(new Column(types.stringType(2), new StringFormatter(2), "CAPUNITS"));
        cols.add(new Column(types.realType(), new RealFormatter(), "WINTHRU"));
        cols.add(new Column(types.realType(), new RealFormatter(), "SPRTHRU"));
        cols.add(new Column(types.realType(), new RealFormatter(), "SUMTHRU"));
        cols.add(new Column(types.realType(), new RealFormatter(), "FALTHRU"));
        cols.add(new Column(types.intType(), new IntegerFormatter(), "HOURS"));
        cols.add(new Column(types.intType(), new IntegerFormatter(), "START"));
        cols.add(new Column(types.intType(), new IntegerFormatter(), "DAYS"));
        cols.add(new Column(types.intType(), new IntegerFormatter(), "WEEKS"));
        cols.add(new Column(types.realType(), new RealFormatter(), "THRUPUT"));
        cols.add(new Column(types.realType(), new RealFormatter(), "MAXRATE"));
        cols.add(new Column(types.realType(), new RealFormatter(), "HEATCON"));
        cols.add(new Column(types.realType(), new RealFormatter(), "SULFCON"));
        cols.add(new Column(types.realType(), new RealFormatter(), "ASHCON"));
        cols.add(new Column(types.realType(), new RealFormatter(), "NETDC"));
        cols.add(new Column(types.intType(), new IntegerFormatter(), "SIC"));
        cols.add(new Column(types.realType(), new RealFormatter(), "LATC"));
        cols.add(new Column(types.realType(), new RealFormatter(), "LONC"));
        cols.add(new Column(types.stringType(2), new StringFormatter(2), "OFFSHORE"));

        return (Column[]) cols.toArray(new Column[0]);
    }
}
