package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.Column;

public interface ColumnsMetadata {

    int[] widths();

    String[] colTypes();

    String[] colNames();

    String identify();

    Column[] cols();
}