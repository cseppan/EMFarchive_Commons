package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.TableDefinition;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.importer.FixedColumnsDataLoader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.importer.temporal.TableFormat;

import java.io.BufferedReader;
import java.sql.SQLException;
import java.util.List;

public class IDAImporter {

    private Datasource datasource;

    public IDAImporter(Datasource datasource) {
        this.datasource = datasource;
    }

    public void run(BufferedReader reader, DatasetTypeUnit unit, List comments, Dataset dataset) throws Exception {
        String table = unit.getInternalSource().getTable();
        try {
            createTable(table, datasource, unit.tableFormat());
        } catch (SQLException e) {
            throw new ImporterException("could not create table for dataset - " + dataset.getName(), e);
        }
        try {
            doImport(reader, unit, comments, dataset, table);
        } 
        catch(Exception e){
            dropTable(table,datasource);
            throw new ImporterException(e.getMessage());
        }
    }
    
    private void createTable(String table, Datasource datasource, TableFormat tableFormat) throws SQLException {
        TableDefinition tableDefinition = datasource.tableDefinition();
        tableDefinition.createTable(table, tableFormat.cols());
    }

    private void dropTable(String table,Datasource datasource) throws ImporterException {
        try {
            TableDefinition def = datasource.tableDefinition();
            def.deleteTable(table);
        } catch (SQLException e) {
            throw new ImporterException(
                    "could not drop table " + table + " after encountering error importing dataset", e);
        }
    }

    private void doImport(BufferedReader reader, DatasetTypeUnit unit, List comments, Dataset dataset, String table)
            throws Exception {
        
        Reader idaReader = new IDAFileReader(reader, unit.fileFormat(), comments);
        FixedColumnsDataLoader loader = new FixedColumnsDataLoader(datasource, unit.tableFormat());

        Record record = idaReader.read();

        List headerComments = idaReader.comments();
        checkHeaderTags(headerComments);

        loader.insertRow(record, dataset, table);
        loader.load(idaReader, dataset, table);
    }

    private void checkHeaderTags(List headerComments) throws Exception {
        checkTag("#IDA", headerComments);
        checkTag("#COUNTRY", headerComments);
        checkTag("#YEAR", headerComments);
        checkTag("#DATA", "#POLID", headerComments);
    }

    private void checkTag(String tag1, String tag2, List headerComments) throws Exception {
        for (int i = 0; i < headerComments.size(); i++) {
            String comment = (String) headerComments.get(i);
            if (comment.trim().startsWith(tag1) || comment.trim().startsWith(tag2))
                return;
        }
        throw new Exception("Could not find tag '" + tag1 + "' or '" + tag2);

    }

    private void checkTag(String tag, List headerComents) throws Exception {
        for (int i = 0; i < headerComents.size(); i++) {
            String comment = (String) headerComents.get(i);
            if (comment.trim().startsWith(tag))
                return;
        }
        throw new Exception("Could not find tag '" + tag + "'");
    }

}
