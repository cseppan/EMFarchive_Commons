package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.OptionalColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.OptionalColumnsTableMetadata;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

public class PointTemporalReferenceImporter {

    private Datasource datasource;

    private OptionalColumnsTableMetadata colsMetadata;

    public PointTemporalReferenceImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.datasource = datasource;
        colsMetadata = new OptionalColumnsTableMetadata(new PointTemporalReferenceColumnsMetadata(
                sqlDataTypes), sqlDataTypes);
    }

    public void run(File file, Dataset dataset) throws ImporterException {
        // FIXME: this table should be created and available ?
        String table = "POINT_SOURCE";

        try {
            doImport(file, dataset, table, colsMetadata);
        } catch (Exception e) {
            throw new ImporterException("could not import File - " + file.getAbsolutePath() + " into Dataset - "
                    + dataset.getName());
        }
    }

    private void doImport(File file, Dataset dataset, String table, OptionalColumnsTableMetadata colsMetadata)
            throws Exception {
        OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, colsMetadata);
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        Reader reader = new PointTemporalReferenceReader(fileReader);

        loader.load(reader, dataset, table);
    }
}
