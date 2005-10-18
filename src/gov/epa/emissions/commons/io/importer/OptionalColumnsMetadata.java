package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.Column;

public interface OptionalColumnsMetadata extends ColumnsMetadata {

    Column[] optionalCols();

    Column[] minCols();

}
