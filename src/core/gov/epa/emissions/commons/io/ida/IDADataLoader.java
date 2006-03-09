package gov.epa.emissions.commons.io.ida;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.TableFormat;
import gov.epa.emissions.commons.io.importer.DataLoader;
import gov.epa.emissions.commons.io.importer.ImporterException;
import gov.epa.emissions.commons.io.importer.Reader;
import gov.epa.emissions.commons.io.temporal.VersionedTableFormat;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class IDADataLoader implements DataLoader {

    private Datasource emissionDatasource;

    private Datasource referenceDatasource;

    private TableFormat tableFormat;

    // TODO: pull out the common code between FixedColumnDataLoader and
    // IDADataLoader
    public IDADataLoader(Datasource emissionDatasource, Datasource referenceDatasource, TableFormat tableFormat) {
        this.emissionDatasource = emissionDatasource;
        this.tableFormat = tableFormat;
        this.referenceDatasource = referenceDatasource;
    }

    public void load(Reader reader, Dataset dataset, String table) throws ImporterException {
        try {
            insertRecords(dataset, table, reader);
        } catch (Exception e) {
            dropData(table, dataset);
            e.printStackTrace();
            throw new ImporterException("Line number " + reader.lineNumber() + ": " + e.getMessage()
                    + "\nCould not load dataset - '" + dataset.getName() + "' into table - " + table);
        }
    }

    private void dropData(String table, Dataset dataset) throws ImporterException {
        try {
            DataModifier modifier = emissionDatasource.dataModifier();
            String key = tableFormat.key();
            long value = dataset.getId();
            modifier.dropData(table, key, value);
        } catch (SQLException e) {
            throw new ImporterException("could not drop data from table " + table, e);
        }
    }

    private void insertRecords(Dataset dataset, String table, Reader reader) throws Exception {
        Record record = reader.read();
        DataModifier modifier = emissionDatasource.dataModifier();
        while (!record.isEnd()) {
            modifier.insertRow(table, data(dataset, record));
            record = reader.read();
        }
    }

    private String[] data(Dataset dataset, Record record) throws SQLException, ImporterException {
        int stateIndex = 1;
        int fipsIndex = 2;
        List data = new ArrayList();
        //FIXME: demo code
        if (tableFormat instanceof VersionedTableFormat){
            addVersionData(data, dataset.getId(), 0);
            stateIndex=4;
            fipsIndex =5;
        }
        else{
            data.add("" + dataset.getId());
        }
        String stateID = record.token(0);
        String fips = fips(stateID, record.token(1));
        data.add(stateIndex, stateAbbr(referenceDatasource, fips));
        data.add(fipsIndex, fips);
        for (int i = 0; i < record.size(); i++)
            data.add(record.token(i));

        addEmptyLineCommentIfNotThere(data,tableFormat);
        return (String[]) data.toArray(new String[0]);
    }
    
    private void addVersionData(List data, long datasetId, int version) {
        data.add(0, "");// record id
        data.add(1, datasetId + "");
        data.add(2, version + "");// version
        data.add(3, "");// delete versions
    }

    private void addEmptyLineCommentIfNotThere(List data, TableFormat tableFormat) throws ImporterException {
        int diff = tableFormat.cols().length- data.size();
        if(diff==1){
            data.add("");
            return;
        }
        throw new ImporterException("Number of tokens are "+data.size() + " but table column size is "+ tableFormat.cols().length);
        
    }

    private String stateAbbr(Datasource referenceDatasource, String fips) throws SQLException {
        String dsName = referenceDatasource.getName();
        String query = "SELECT fips.state_abbr FROM " + dsName
                + ".fips WHERE fips.country_code='US' AND fips.state_county_fips='" + fips + "'";
        ResultSet rs = referenceDatasource.query().executeQuery(query);
        try {
            rs.next();
            return rs.getString(1);
        } catch (SQLException e) {
            throw new SQLException("No State abbr found:\nQuery-" + query + "\n" + e.getMessage());
        }
    }

    private String fips(String stateID, String countyID) {
        stateID = stateID.trim();
        countyID = countyID.trim();
        // assume max length of stateID is 2 and countyID is 3
        return pad(stateID, 2) + pad(countyID, 3);
    }

    private String pad(String value, int padLength) {
        int initLength = value.length();
        StringBuffer sb = new StringBuffer(value);
        for (int i = initLength; i < padLength; i++) {
            sb.insert(0, '0');
        }
        return sb.toString();
    }

}
