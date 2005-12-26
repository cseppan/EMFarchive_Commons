package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.OptionalColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Iterator;
import java.util.List;

public class PointTemporalReferenceImporter implements Importer {
    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private DatasetTypeUnit unit;

    /**
     * Expects table 'POINT_SOURCE' to be available in Datasource
     */
    public PointTemporalReferenceImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) {
        this.file = file;
        this.dataset = dataset;
        this.datasource = datasource;
        PointTemporalReferenceFileFormat fileFormat = new PointTemporalReferenceFileFormat(sqlDataTypes);
        TableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        unit = new DatasetTypeUnit(tableFormat, fileFormat);
    }

    public void run() throws ImporterException {
        try {
            doImport(file, dataset, "POINT_SOURCE", unit);
        } catch (Exception e) {
            throw new ImporterException(e.getMessage() + " Filename: " + file.getAbsolutePath() + "\n");
        }
    }

    private void doImport(File file, Dataset dataset, String table, DatasetTypeUnit unit) throws Exception {
        DataLoader loader = new OptionalColumnsDataLoader(datasource, (FileFormatWithOptionalCols) unit.fileFormat(),
                unit.tableFormat().key());
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        Reader reader = new PointTemporalReferenceReader(fileReader);

        loader.load(reader, dataset, table);
        loadDataset(file, table, unit.tableFormat(), reader, dataset);
    }

    private void loadDataset(File file, String table, TableFormat format, Reader reader, Dataset dataset) {
        // TODO: other properties ?
        HelpImporter delegate = new HelpImporter();
        delegate.setInternalSource(file, table, format, dataset);
        dataset.setDescription(descriptions(reader.comments()));
    }

    private String descriptions(List comments) {
        StringBuffer description = new StringBuffer();
        for (Iterator iter = comments.iterator(); iter.hasNext();)
            description.append(iter.next() + "\n");

        return description.toString();
    }

}
