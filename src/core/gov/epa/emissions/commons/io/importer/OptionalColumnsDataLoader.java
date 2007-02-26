package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.data.Dataset;
import gov.epa.emissions.commons.db.OptimizedTableModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class OptionalColumnsDataLoader implements DataLoader {

    private Datasource datasource;

    private FileFormatWithOptionalCols fileFormat;

    private String key;

    public OptionalColumnsDataLoader(Datasource datasource, FileFormatWithOptionalCols format, String key) {
        this.datasource = datasource;
        this.fileFormat = format;
        this.key = key;
    }

    public void load(Reader reader, Dataset dataset, String table) throws ImporterException {
        OptimizedTableModifier dataModifier = null;
        try {
            dataModifier = dataModifier(datasource, table);
            insertRecords(dataset, reader, dataModifier);
        } catch (Exception e) {
            e.printStackTrace();
            dropData(table, dataset, dataModifier);
            int lineNum = reader.lineNumber();
            throw new ImporterException("Line number " + lineNum + ": " + e.getMessage() + "\nLine: "
                    + reader.line() + "\nCould not load dataset - '" + dataset.getName() + "' into table - " + table);
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
        } catch (SQLException e) {
            throw new ImporterException(e.getMessage());
        }
    }

    private void dropData(String table, Dataset dataset, OptimizedTableModifier dataModifier) throws ImporterException {
        try {
            long value = dataset.getId();
            dataModifier.dropData(key, value);
        } catch (SQLException e) {
            throw new ImporterException("could not drop data from table " + table + "\n" + e.getMessage(), e);
        }
    }

    private void insertRecords(Dataset dataset, Reader reader, OptimizedTableModifier dataModifier) throws Exception {
        dataModifier.start();
        try {
            for (Record record = reader.read(); !record.isEnd(); record = reader.read()) {
                int minColsSize = fileFormat.minCols().length;
                if (record.size() < minColsSize)
                    throw new ImporterException("The number of tokens in the line are " + record.size()
                            + ", It's less than minimum number of columns expected(" + minColsSize + ")");
                String[] data = data(dataset, record, fileFormat);

                dataModifier.insert(data);

            }
        } catch (SQLException e) {
            throw new ImporterException("Error processing insert query: " + e.getMessage());
        } finally {
            dataModifier.finish();
        }
    }

    private String[] data(Dataset dataset, Record record, FileFormatWithOptionalCols format) {
        List data = new ArrayList();
        data.addAll(record.tokens());
        format.fillDefaults(data, dataset.getId());
        massageNullMarkers(data);
        return (String[]) data.toArray(new String[0]);
    }

    // FIXME: should this be applied to ALL data loaders ?
    private void massageNullMarkers(List data) {
        for (int i = 0; i < data.size(); i++) {
            String element = (String) data.get(i);
            if (element.equals("-9"))// NULL marker
                data.set(i, "");
        }
    }

}
