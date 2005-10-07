package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.DataModifier;
import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.io.Dataset;
import gov.epa.emissions.commons.io.importer.temporal.Record;

import java.util.ArrayList;
import java.util.List;

public class DelimitedFileLoader {

	private Datasource datasource;

	public DelimitedFileLoader(Datasource datasource){
		this.datasource = datasource;
	}

	public void load(Dataset dataset, DelimitedFileReader reader) throws ImporterException {
		String table = "SimpleDelimited";
        try {
            insertRecords(table, dataset, reader);
        } catch (Exception e) {
            throw new ImporterException("could not load dataset - '" + dataset.getName() + "' into table - " + table, e);
        }
		
	}
	
	private void insertRecords(String table, Dataset dataset, DelimitedFileReader reader) throws Exception {
        Record record = reader.read();
        DataModifier modifier = datasource.getDataModifier();
        String qualifiedTable = datasource.getName() + "." + table;
        String varcharType ="VARCHAR(20)";//FIXME: get it from metadata
        String [] colTypes = {varcharType,varcharType,varcharType,varcharType,varcharType,varcharType, varcharType};
        while (!record.isEnd()) {
        	
            modifier.insertRow(qualifiedTable, data(record), colTypes);
            record = reader.read();
        }
    }
	
	private String[] data(Record record) {
        List data = new ArrayList();
        for (int i = 0; i < record.size(); i++) {
            data.add(record.token(i));
        }
        return (String[]) data.toArray(new String[0]);
    }

	
}
