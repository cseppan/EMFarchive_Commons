package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.temporal.TableFormat;

public interface FormatUnit {

    FileFormat fileFormat();

    TableFormat tableFormat();

    boolean isRequired();

    void setInternalSource(InternalSource internalSource);

    InternalSource getInternalSource();
}