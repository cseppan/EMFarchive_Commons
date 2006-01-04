package gov.epa.emissions.commons.io.nif.nonpointNonroad;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DataFormatFactory;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.DataTable;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.NonVersionedDataFormatFactory;
import gov.epa.emissions.commons.io.nif.NIFImporter;

import java.io.File;

public class NIFNonRoadImporter implements Importer {

    private NIFImporter delegate;

    public NIFNonRoadImporter(File folder, String[] filePatterns, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes)
            throws ImporterException {
        this(folder, filePatterns, dataset, datasource, sqlDataTypes, new NonVersionedDataFormatFactory());
    }

    public NIFNonRoadImporter(File folder, String[] filePatterns, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes,
            DataFormatFactory factory) throws ImporterException {
        File[] files = new File[filePatterns.length];
        for(int i = 0; i < filePatterns.length; i++) 
            files[i] = new File(folder, filePatterns[i]);
        String tablePrefix = new DataTable(dataset, datasource).name();
        delegate = new NIFImporter(files, dataset,
                new NIFNonRoadFileDatasetTypeUnits(files, tablePrefix, sqlDataTypes, factory), datasource);

    }

    public void run() throws ImporterException {
        delegate.run();
    }

}
