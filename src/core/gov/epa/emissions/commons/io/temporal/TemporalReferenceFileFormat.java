package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.IntegerFormatter;
import gov.epa.emissions.commons.io.StringFormatter;

import java.util.ArrayList;
import java.util.List;

public class TemporalReferenceFileFormat implements FileFormat {

    private Column[] cols;

    public TemporalReferenceFileFormat(SqlDataTypes types) {
        this.cols = createCols(types);
    }

    public String identify() {
        return "Temporal Cross-Reference";
    }

    public Column[] cols() {
        return cols;
    }

    public Column[] createCols(SqlDataTypes types) {
        List column = new ArrayList();
        
        column.add(new Column("SCC", types.stringType(10), new StringFormatter(10)));
        column.add(new Column("Monthly_Code", types.intType(), new IntegerFormatter()));
        column.add(new Column("Weekly_Code", types.intType(), new IntegerFormatter()));
        column.add(new Column("Diurnal_Code", types.intType(), new IntegerFormatter()));
        column.add(new Column("Pollutants", types.stringType(32), new StringFormatter(32)));
        column.add(new Column("FIPS", types.stringType(32), new StringFormatter(32)));
        column.add(new Column("LinkID_PlantID", types.stringType(32), new StringFormatter(32)));
        column.add(new Column("Characteristic_1", types.stringType(32), new StringFormatter(32)));
        column.add(new Column("Characteristic_2", types.stringType(32), new StringFormatter(32)));
        column.add(new Column("Characteristic_3", types.stringType(32), new StringFormatter(32)));
        column.add(new Column("Characteristic_4", types.stringType(32), new StringFormatter(32)));
        column.add(new Column("Characteristic_5", types.stringType(32), new StringFormatter(32)));
        
        return (Column[]) column.toArray(new Column[0]);
    }
}
