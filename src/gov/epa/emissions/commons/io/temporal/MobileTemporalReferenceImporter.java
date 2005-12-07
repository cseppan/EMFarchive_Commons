package gov.epa.emissions.commons.io.temporal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnitWithOptionalCols;
import gov.epa.emissions.commons.io.importer.Importer;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.OptionalColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.SimpleTableFormatWithOptionalCols;
import gov.epa.emissions.commons.io.importer.TableFormatWithOptionalCols;

public class MobileTemporalReferenceImporter implements Importer {
    private static Log log = LogFactory.getLog(MobileTemporalReferenceImporter.class);

    private Dataset dataset;

    private Datasource datasource;

    private File file;

    private DatasetTypeUnitWithOptionalCols unit;

    public MobileTemporalReferenceImporter(File file, Dataset dataset, Datasource datasource, SqlDataTypes sqlDataTypes) throws ImporterException {
        setup(file);
        this.dataset = dataset;
        this.datasource = datasource;
        MobileTemporalReferenceFileFormat fileFormat = new MobileTemporalReferenceFileFormat(sqlDataTypes);
        TableFormatWithOptionalCols tableFormat = new SimpleTableFormatWithOptionalCols(fileFormat, sqlDataTypes);
        unit = new DatasetTypeUnitWithOptionalCols(tableFormat, fileFormat);
    }
    
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
        try {
            doImport(file, dataset, "MOBILE_SOURCE", (TableFormatWithOptionalCols) unit.tableFormat());
        } catch (Exception e) {
            throw new ImporterException(e.getMessage() + " Filename: " + file.getAbsolutePath() + "\n");

        }
    }

    private void doImport(File file, Dataset dataset, String table, TableFormatWithOptionalCols tableFormat)
            throws Exception {
        OptionalColumnsDataLoader loader = new OptionalColumnsDataLoader(datasource, tableFormat);
        BufferedReader fileReader = new BufferedReader(new FileReader(file));
        Reader reader = new TemporalReferenceReader(fileReader);

        loader.load(reader, dataset, table);
        loadDataset(reader, dataset);
    }

    private void loadDataset(Reader reader, Dataset dataset) {
        // TODO: other properties ?
        dataset.setDescription(descriptions(reader.comments()));
    }

    private String descriptions(List comments) {
        StringBuffer description = new StringBuffer();
        for (Iterator iter = comments.iterator(); iter.hasNext();)
            description.append(iter.next() + "\n");

        return description.toString();
    }
}
