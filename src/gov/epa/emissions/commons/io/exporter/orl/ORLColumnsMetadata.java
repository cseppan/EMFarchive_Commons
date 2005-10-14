package gov.epa.emissions.commons.io.exporter.orl;

import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.importer.ColumnsMetadata;

public interface ORLColumnsMetadata extends ColumnsMetadata {

    Column[] cols();

}
