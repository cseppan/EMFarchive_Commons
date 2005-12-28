package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.ExportStatement;
import gov.epa.emissions.commons.io.FileFormat;
import gov.epa.emissions.commons.io.FixedColsTableFormat;
import gov.epa.emissions.commons.io.SimpleExportStatement;
import gov.epa.emissions.commons.io.TableFormat;

public class FixedDataFormatFactory implements DataFormatFactory {

    public TableFormat tableFormat(FileFormat fileFormat, SqlDataTypes sqlDataTypes) {
        return new FixedColsTableFormat(fileFormat, sqlDataTypes);
    }

    public FillDefaultValues defaultValuesFiller() {
        return null;
    }

    public ExportStatement exportStatement() {
        return new SimpleExportStatement();
    }

}
