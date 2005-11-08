package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.io.importer.FileFormat;

public interface IDAFileFormat extends FileFormat{
    
    public void addPollutantCols(String[] pollutants);
}
