package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.CharFormatter;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.OptionalColumnsMetadata;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PointTemporalReferenceColumnsMetadata implements OptionalColumnsMetadata {

    private SqlDataTypes types;

    public PointTemporalReferenceColumnsMetadata(SqlDataTypes types) {
        this.types = types;
    }

    public int[] widths() {
        return null;
    }

    public String[] colNames() {
        Column[] cols = cols();

        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            names.add(cols[i].name());
        }

        return (String[]) names.toArray(new String[0]);
    }

    public String[] colTypes() {
        Column[] cols = cols();

        List sqlTypes = new ArrayList();
        for (int i = 0; i < cols.length; i++) {
            sqlTypes.add(cols[i].sqlType());
        }

        return (String[]) sqlTypes.toArray(new String[0]);
    }

    public String identify() {
        return "Point - Temporal Reference";
    }

    public Column[] cols() {
        Column[] minCols = minCols();
        Column[] optionalCols = optionalCols();

        return asArray(minCols, optionalCols);
    }

    private Column[] asArray(Column[] minCols, Column[] optionalCols) {
        List list = new ArrayList();
        list.addAll(Arrays.asList(minCols));
        list.addAll(Arrays.asList(optionalCols));

        return (Column[]) list.toArray(new Column[0]);
    }

    public Column[] optionalCols() {
        Column pollutants = new Column(types.charType(), new CharFormatter(), "Pollutants");
        Column cscCode = new Column(types.intType(), new IntegerFormatter(), "CSC_Code");
        Column plantId = new Column(types.charType(), new CharFormatter(), "Plant_Id");
        Column characteristic1 = new Column(types.charType(), new CharFormatter(), "Characteristic_1");
        Column characteristic2 = new Column(types.charType(), new CharFormatter(), "Characteristic_2");
        Column characteristic3 = new Column(types.charType(), new CharFormatter(), "Characteristic_3");
        Column characteristic4 = new Column(types.charType(), new CharFormatter(), "Characteristic_4");
        Column characteristic5 = new Column(types.charType(), new CharFormatter(), "Characteristic_5");

        return new Column[] { pollutants, cscCode, plantId, characteristic1, characteristic2, characteristic3,
                characteristic4, characteristic5 };
    }

    public Column[] minCols() {
        Column scc = new Column(types.stringType(10), new StringFormatter(10), "SCC");
        Column monthlyCode = new Column(types.intType(), new IntegerFormatter(), "Monthly_Code");
        Column weeklyCode = new Column(types.intType(), new IntegerFormatter(), "Weekly_Code");
        Column diurnalCode = new Column(types.intType(), new IntegerFormatter(), "Diurnal_Code");

        return new Column[] { scc, monthlyCode, weeklyCode, diurnalCode };
    }
}
