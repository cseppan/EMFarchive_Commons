package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.orl.ORLImporter;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//FIXME: move the functionality to appropriate components/classes. Remove this class.
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
        return format(datasetName).trim().replaceAll(" ", "_");
    }

    private String format(String name) {
        String result = name;

        for (int i = 0; i < result.length(); i++) {
            if (!Character.isJavaLetterOrDigit(result.charAt(i))) {
                result = result.replace(result.charAt(i), '_');
            }
        }

        if (Character.isDigit(result.charAt(0))) {
            result = result.replace(result.charAt(0), '_');
            result = "DS" + result;
        }

        return result;
    }

    public void createTable(String table, Datasource datasource, TableFormat tableFormat)
            throws ImporterException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        try {
            tableDefinition.createTable(table, tableFormat.cols());
        } catch (SQLException e) {
            throw new ImporterException("could not create table - " + table + "\n" + e.getMessage(),
                    e);
        }
    }

    public void dropTable(String table, Datasource datasource) throws ImporterException {
        try {
            TableDefinition def = datasource.tableDefinition();
            def.dropTable(table);
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

    public void setInternalSource(File file, String table, TableFormat tableFormat, Dataset dataset) {
        InternalSource source = new InternalSource();
        source.setTable(table);
        source.setType(tableFormat.identify());
        source.setCols(colNames(tableFormat.cols()));
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

    public InternalSource summarySource(String datasetName) {
        InternalSource source = new InternalSource();
        source.setType("Summary Table");
        source.setTable(tableName(datasetName) + "_summary");
        source.setSource("TODO: get a name");

        return source;
    }

}
