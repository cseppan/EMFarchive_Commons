package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.importer.FillDefaultValues;

public interface DataFormatFactory {

    TableFormat tableFormat(FileFormat fileFormat, SqlDataTypes sqlDataTypes);

    FillDefaultValues defaultValuesFiller();

    ExportStatement exportStatement();

}
