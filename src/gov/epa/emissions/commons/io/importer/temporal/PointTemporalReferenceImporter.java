package gov.epa.emissions.commons.io.importer.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.NewImporter;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.OptionalColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.OptionalColumnsTableMetadata;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Iterator;
import java.util.List;

public class PointTemporalReferenceImporter implements NewImporter {

    private Datasource datasource;

    private OptionalColumnsTableMetadata colsMetadata;

    public PointTemporalReferenceImporter(Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.datasource = datasource;
        colsMetadata = new OptionalColumnsTableMetadata(new PointTemporalReferenceColumnsMetadata(sqlDataTypes),
                sqlDataTypes);
    }

    /**
     * Expects table 'POINT_SOURCE' to be available in Datasource
     */
    public void run(File file, Dataset dataset) throws ImporterException {
        try {
            doImport(file, dataset, "POINT_SOURCE", colsMetadata);
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
        loadDataset(reader, dataset);
    }

    private void loadDataset(Reader reader, Dataset dataset) {
        //TODO: other properties ?
        dataset.setDescription(descriptions(reader.comments()));
    }

    private String descriptions(List comments) {
        StringBuffer description = new StringBuffer();
        for (Iterator iter = comments.iterator(); iter.hasNext();)
            description.append(iter.next() + "\n");

        return description.toString();
    }
}
