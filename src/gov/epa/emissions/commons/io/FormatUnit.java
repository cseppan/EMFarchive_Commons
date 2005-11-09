package gov.epa.emissions.commons.io;

import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.temporal.TableFormat;

public interface FormatUnit {

    public FileFormat fileFormat();

    public TableFormat tableFormat();
    
    public boolean isRequired();
    
    public void setInternalSource(InternalSource internalSource);
    
    public InternalSource getInternalSource();
}