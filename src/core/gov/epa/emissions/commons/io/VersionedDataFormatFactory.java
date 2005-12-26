package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.temporal.VersionedTableFormat;

public class VersionedDataFormatFactory implements DataFormatFactory {

    public TableFormat tableFormat(FileFormat fileFormat, SqlDataTypes sqlDataTypes) {
        return new VersionedTableFormat(fileFormat, sqlDataTypes);
    }

    public FillDefaultValues defaultValuesFiller() {
        return new FillDefaultValuesOfVersionedRecord();
    }

}
