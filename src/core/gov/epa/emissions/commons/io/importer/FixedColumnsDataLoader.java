package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.OptimizedTableModifier;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.temporal.VersionedTableFormat;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class FixedColumnsDataLoader implements DataLoader {

    private Datasource datasource;

    private TableFormat tableFormat;

    public FixedColumnsDataLoader(Datasource datasource, TableFormat tableFormat) {
        this.datasource = datasource;
        this.tableFormat = tableFormat;
    }

    public void load(Reader reader, Dataset dataset, String table) throws ImporterException {
        OptimizedTableModifier dataModifier = null;
        try {
            dataModifier = dataModifier(datasource, table);
            insertRecords(dataset, reader, dataModifier);
        } catch (Exception e) {
            e.printStackTrace();
            dropData(table, dataset, dataModifier);
            throw new ImporterException(e.getMessage()
                    + "\nCould not load dataset - '" + dataset.getName() + "' into table - " + table);
        } finally {
            close(dataModifier);
        }
    }

    private OptimizedTableModifier dataModifier(Datasource datasource, String table) throws ImporterException {
        try {
            return new OptimizedTableModifier(datasource, table);
        } catch (SQLException e) {
            throw new ImporterException(e.getMessage());
        }
    }

    private void close(OptimizedTableModifier dataModifier) throws ImporterException {
        try {
            if (dataModifier != null)
                dataModifier.close();
        } catch (Exception e) {
            throw new ImporterException(e.getMessage());
        }
    }

    private void dropData(String table, Dataset dataset, OptimizedTableModifier dataModifier) throws ImporterException {
        try {
            String key = tableFormat.key();
            long value = dataset.getId();
            dataModifier.dropData(key, value);
        } catch (SQLException e) {
            throw new ImporterException("could not drop data from table " + table, e);
        }
    }

    private void insertRecords(Dataset dataset, Reader reader, OptimizedTableModifier dataModifier) throws Exception {
        dataModifier.start();
        try {
            Record record = reader.read();
            while (!record.isEnd()) {
                // TODO: check to see if the records of type string are not too long for the format
                // can you get the field length info from the TableFormat? or do you need the FileFormat?
                // if need FileFormat, is it OK to pass in?
                checkDataLengths(record);

                dataModifier.insert(data(dataset, record));
                record = reader.read();
            }
        }catch (ImporterException e) {
            throw new ImporterException(e.getMessage());
        }finally {
            dataModifier.finish();
        }
    }

    private void checkDataLengths(Record record) throws Exception
    {
        int firstCol = tableFormat.getOffset();
        int offSet = tableFormat.getOffset();
        
        Column [] columns = tableFormat.cols();
        
        for (int c = firstCol; c < firstCol+tableFormat.getBaseLength(); c++)
        {
            Column col = columns[c];
            //System.out.println("c="+c+", column name = "+col.name()+", type="+col.sqlType());
            if (col.sqlType().toLowerCase().startsWith("varchar"))
            {  
               String item = record.token(c-offSet);
               if (col.width() < item.length())
                   throw new ImporterException ("Value "+item+" is too large for the column "+ col.name());
            }           
        }
    }
    
    protected String[] data(Dataset dataset, Record record) {
        List data = new ArrayList();

        if (tableFormat instanceof VersionedTableFormat)
            addVersionData(data, dataset.getId(), 0);
        else
            data.add("" + dataset.getId());

        data.addAll(record.tokens());

        massageNullMarkers(data);

        return (String[]) data.toArray(new String[0]);
    }

    protected void addVersionData(List data, long datasetId, int version) {
        data.add(0, "");// record id
        data.add(1, datasetId + "");
        data.add(2, version + "");// version
        data.add(3, "");// delete versions
    }

    protected void massageNullMarkers(List data) {
        for (int i = 0; i < data.size(); i++) {
            String element = (String) data.get(i);
            if (element.equals("-9"))// NULL marker
                data.set(i, "");
        }
    }

}
