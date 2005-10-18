package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public interface OptionalColumnsMetadata extends ColumnsMetadata {

    Column[] optionalCols();

    Column[] minCols();

}
