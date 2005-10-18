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
        Column pollutants = new Column("Pollutants", types.charType(), new CharFormatter());
        Column cscCode = new Column("CSC_Code", types.intType(), new IntegerFormatter());
        Column plantId = new Column("Plant_Id", types.charType(), new CharFormatter());
        Column characteristic1 = new Column("Characteristic_1", types.charType(), new CharFormatter());
        Column characteristic2 = new Column("Characteristic_2", types.charType(), new CharFormatter());
        Column characteristic3 = new Column("Characteristic_3", types.charType(), new CharFormatter());
        Column characteristic4 = new Column("Characteristic_4", types.charType(), new CharFormatter());
        Column characteristic5 = new Column("Characteristic_5", types.charType(), new CharFormatter());

        return new Column[] { pollutants, cscCode, plantId, characteristic1, characteristic2, characteristic3,
                characteristic4, characteristic5 };
    }

    public Column[] minCols() {
        Column scc = new Column("SCC", types.stringType(10), new StringFormatter(10));
        Column monthlyCode = new Column("Monthly_Code", types.intType(), new IntegerFormatter());
        Column weeklyCode = new Column("Weekly_Code", types.intType(), new IntegerFormatter());
        Column diurnalCode = new Column("Diurnal_Code", types.intType(), new IntegerFormatter());

        return new Column[] { scc, monthlyCode, weeklyCode, diurnalCode };
    }
}
