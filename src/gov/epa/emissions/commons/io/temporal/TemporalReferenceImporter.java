package gov.epa.emissions.commons.io.temporal;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.FormatUnit;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.HelpImporter;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TemporalReferenceImporter implements Importer {
    private static Log log = LogFactory.getLog(PointTemporalReferenceImporter.class);

    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private FormatUnit unit;
    
    private HelpImporter delegate;

    public TemporalReferenceImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) throws ImporterException {
        setup(file);
        this.dataset = dataset;
        this.datasource = datasource;
        FileFormat fileFormat = new TemporalReferenceFileFormat(sqlDataTypes);
        TableFormat tableFormat = new VersionedTemporalReferenceTableFormat(fileFormat, sqlDataTypes);
        unit = new DatasetTypeUnit(tableFormat, fileFormat);
        this.delegate = new HelpImporter();
    }

    /**
     * Expects table 'AREA_SOURCE' to be available in Datasource
     */
    private void setup(File file) throws ImporterException {
        this.file = validateFile(file);
    }

    private File validateFile(File file) throws ImporterException {
        log.debug("check if file exists " + file.getAbsolutePath());
        if (!file.exists() || !file.isFile()) {
            log.error("File " + file.getAbsolutePath() + " not found");
            throw new ImporterException("File not found");
        }
        return file;
    }

    public void run() throws ImporterException {
        String table = delegate.tableName(dataset.getName());
        delegate.createTable(table,datasource,unit.tableFormat(),dataset.getName());
        try {
            doImport(file, dataset, table, (VersionedTemporalReferenceTableFormat)unit.tableFormat());
        } catch (Exception e) {
            throw new ImporterException(e.getMessage() + " Filename: " + file.getAbsolutePath() + "\n");

        }
    }

    private void doImport(File file, Dataset dataset, String table, VersionedTemporalReferenceTableFormat tableFormat)
            throws Exception {
        VersionedTemporalReferenceDataLoader loader = new VersionedTemporalReferenceDataLoader(datasource, tableFormat);
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        Reader reader = new TemporalReferenceReader(fileReader);

        loader.load(reader, dataset, table);
        addVersionZeroEntryToVersionsTable(datasource, dataset);
        loadDataset(file, table, unit.fileFormat(), reader, dataset);
    }
 
    private void addVersionZeroEntryToVersionsTable(Datasource datasource, Dataset dataset) throws SQLException {
        DataModifier modifier = datasource.dataModifier();
        String[] data = { dataset.getDatasetid() + "", "0", "Initial Version", "", "true" };
        modifier.insertRow("versions", data);
    }
    
    private void loadDataset(File file, String table, FileFormat fileFormat, Reader reader, Dataset dataset) {
        // TODO: other properties ?
        HelpImporter delegate = new HelpImporter();
        delegate.setInternalSource(file, table, fileFormat, dataset);
        dataset.setDescription(descriptions(reader.comments()));
    }

    private String descriptions(List comments) {
        StringBuffer description = new StringBuffer();
        for (Iterator iter = comments.iterator(); iter.hasNext();)
            description.append(iter.next() + System.getProperty("line.separator"));

        return description.toString();
    }
}
