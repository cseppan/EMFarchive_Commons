package gov.epa.emissions.commons.io.nif;

import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.ImporterException;

import java.io.File;

public interface NIFDatasetTypeUnits {

    public  void process(File[] files, String tableName) throws ImporterException;

    public FormatUnit[] formatUnits();

}