package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.db.version.Version;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.ExportStatement;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.VersionedExportStatement;
import gov.epa.emissions.commons.io.temporal.VersionedTableFormat;

public class VersionedDataFormatFactory implements DataFormatFactory {

    private Version version;

    public VersionedDataFormatFactory(Version version) {
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
