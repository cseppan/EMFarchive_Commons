package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.temporal.VersionedTableFormat;

public class VersionedDataFormatFactory implements DataFormatFactory {

    private int version;

    public VersionedDataFormatFactory(int version) {
        this.version = version;
    }

    public TableFormat tableFormat(FileFormat fileFormat, SqlDataTypes sqlDataTypes) {
        return new VersionedTableFormat(fileFormat, sqlDataTypes);
    }

    public FillDefaultValues defaultValuesFiller() {
        return new FillDefaultValuesOfVersionedRecord();
    }

    public ExportStatement exportStatement() {
        return new VersionedExportStatement(version);
    }

}
