package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.Column;

public interface ColumnsMetadata {

    String identify();

    Column[] cols();
}