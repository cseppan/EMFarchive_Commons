package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.NullFormatter;
import gov.epa.emissions.commons.io.RealFormatter;
import gov.epa.emissions.commons.io.importer.FillDefaultValues;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ORLCoSTPointFileFormat extends ORLPointFileFormat {

    public ORLCoSTPointFileFormat(SqlDataTypes types) {
        super(types);
    }

    public ORLCoSTPointFileFormat(SqlDataTypes types, FillDefaultValues filler) {
        super(types, filler);
    }

    public Column[] optionalCols() {
        List cols = new ArrayList(Arrays.asList(super.optionalCols()));

        //new columns used for control strategy runs...
        cols.add(new Column("DESIGN_CAPACITY", types.realType(), new RealFormatter()));
        cols.add(new Column("CONTROL_MEASURES", types.text(), new NullFormatter(), "DEFAULT ''::text"));
        cols.add(new Column("PCT_REDUCTION", types.text(), new NullFormatter(), "DEFAULT ''::text"));
        cols.add(new Column("TOTAL_COST", types.realType(), new RealFormatter()));


        return (Column[]) cols.toArray(new Column[0]);
    }
}
