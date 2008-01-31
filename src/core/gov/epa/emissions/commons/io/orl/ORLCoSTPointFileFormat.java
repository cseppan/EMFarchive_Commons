package gov.epa.emissions.commons.io.orl;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.FillDefaultValues;

public class ORLCoSTPointFileFormat extends ORLPointFileFormat {

    public ORLCoSTPointFileFormat(SqlDataTypes types) {
        super(types);
    }

    public ORLCoSTPointFileFormat(SqlDataTypes types, FillDefaultValues filler) {
        super(types, filler);
    }
}
