package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;
import gov.epa.emissions.commons.io.orl.ORLImporter;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class HelpImporter {

    private static Log log = LogFactory.getLog(ORLImporter.class);

    public void validateFile(File file) throws ImporterException {
        log.debug("check if file exists " + file.getName());
        if (!file.exists()) {
            log.error("File " + file.getAbsolutePath() + " not found");
            throw new ImporterException("The file '" + file + "' does not exist");
        }

        if (!file.isFile()) {
            log.error("File " + file.getAbsolutePath() + " is not a file");
            throw new ImporterException("The file '" + file + "' is not a file");
        }
    }

    public String tableName(String datasetName) {
        return datasetName.trim().replaceAll(" ", "_");
    }

    public void createTable(String table, Datasource datasource, TableFormat tableFormat, String datasetName)
            throws ImporterException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        try {
            tableDefinition.createTable(table, tableFormat.cols());
        } catch (SQLException e) {
            throw new ImporterException("could not create table for dataset - " + datasetName, e);
        }

    }

    public void dropTable(String table, Datasource datasource) throws ImporterException {
        try {
            TableDefinition def = datasource.tableDefinition();
            def.deleteTable(table);
        } catch (SQLException e) {
            throw new ImporterException(
                    "could not drop table " + table + " after encountering error importing dataset", e);
        }
    }

    public String descriptions(List comments) {
        StringBuffer description = new StringBuffer();
        for (Iterator iter = comments.iterator(); iter.hasNext();)
            description.append(iter.next() + "\n");

        return description.toString();
    }

    public void setInternalSource(File file, String table, FileFormat fileFormat, Dataset dataset) {
        InternalSource source = new InternalSource();
        source.setTable(table);
        source.setType(fileFormat.identify());
        source.setCols(colNames(fileFormat.cols()));
        source.setSource(file.getAbsolutePath());
        source.setSourceSize(file.length());

        dataset.addInternalSource(source);
    }

    private String[] colNames(Column[] cols) {
        List names = new ArrayList();
        for (int i = 0; i < cols.length; i++)
            names.add(cols[i].name());

        return (String[]) names.toArray(new String[0]);
    }

}
