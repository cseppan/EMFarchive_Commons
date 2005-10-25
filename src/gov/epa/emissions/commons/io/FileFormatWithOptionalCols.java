package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.FileFormat;

public interface FileFormatWithOptionalCols extends FileFormat {

    Column[] optionalCols();

    Column[] minCols();

}
