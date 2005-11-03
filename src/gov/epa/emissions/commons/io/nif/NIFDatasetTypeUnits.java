package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.ImporterException;

public interface NIFDatasetTypeUnits {

    public  void processFiles(InternalSource[] internalSources) throws ImporterException;

    public FormatUnit[] formatUnits();

}