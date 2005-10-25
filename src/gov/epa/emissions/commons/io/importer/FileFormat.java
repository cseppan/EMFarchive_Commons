package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.io.Column;

public interface FileFormat {

    String identify();

    Column[] cols();
}