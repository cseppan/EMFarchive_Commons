package gov.epa.emissions.commons.io.importer.generic;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.StringFormatter;
import gov.epa.emissions.commons.io.importer.FileFormat;

public class LineFileFormat implements FileFormat {

    private Column[] columns;

    public LineFileFormat(SqlDataTypes typeMapper) {
        columns = createCols(typeMapper);
    }

    public String identify() {
        return "Line Importer";
    }

    public Column[] cols() {
        return columns;
    }

    private Column[] createCols(SqlDataTypes types) {
        return new Column[]{new Column("Col_1", types.stringType(256), new StringFormatter(256))};

    }
}
