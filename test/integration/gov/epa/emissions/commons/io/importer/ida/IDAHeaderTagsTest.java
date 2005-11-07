package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.DatasetTypeUnit;
import gov.epa.emissions.commons.io.InternalSource;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.DbTestCase;
import gov.epa.emissions.commons.io.importer.temporal.FixedColsTableFormat;
import gov.epa.emissions.framework.db.DbUpdate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Random;

public class IDAHeaderTagsTest extends DbTestCase {

	private SqlDataTypes sqlDataTypes;

	private Datasource datasource;

	private SimpleDataset dataset;

	protected void setUp() throws Exception {
		super.setUp();
		DbServer dbServer = dbSetup.getDbServer();
		sqlDataTypes = dbServer.getDataType();
		datasource = dbServer.getEmissionsDatasource();

		dataset = new SimpleDataset();
		dataset.setName("test");
		dataset.setDatasetid(new Random().nextLong());
	}

	private void dropTable() throws Exception {
		DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
		dbUpdate.dropTable(datasource.getName(), dataset.getName());
	}

	public void testShouldIdentifyAllRequiredTags() throws Exception {
	    String source = "test/data/ida/small-area.txt";
        InternalSource internalSource = internalSource(source);
        dataset.setInternalSources(new InternalSource[] { internalSource });
		BufferedReader reader = new BufferedReader(new FileReader(source));
		IDAHeaderReader headerReader = new IDAHeaderReader(reader);
		headerReader.read();

		DatasetTypeUnit unit = createUnit(headerReader);
        unit.setInternalSource(internalSource);
        
		IDAImporter importer = new IDAImporter(datasource);
		importer.run(reader, unit, headerReader.comments(), dataset);
        dropTable();
	}

    private DatasetTypeUnit createUnit(IDAHeaderReader headerReader) {
        FileFormat fileFormat = new IDAAreaFileFormat(headerReader
				.polluntants(), sqlDataTypes);
        FixedColsTableFormat tableFormat = new FixedColsTableFormat(fileFormat, sqlDataTypes);
        DatasetTypeUnit unit = new DatasetTypeUnit(tableFormat, fileFormat);
        return unit;
    }

	public void testShouldIdentifyNoIDATag() throws Exception {
		String source = "test/data/ida/noIDATags.txt";
        InternalSource internalSource = internalSource(source);
        dataset.setInternalSources(new InternalSource[] { internalSource });
        
		BufferedReader reader = new BufferedReader(new FileReader(source));
		IDAHeaderReader headerReader = new IDAHeaderReader(reader);
		headerReader.read();
        DatasetTypeUnit unit = createUnit(headerReader);
        unit.setInternalSource(internalSource);
		IDAImporter importer = new IDAImporter(datasource);
		try {
			importer
					.run(reader, unit, headerReader.comments(), dataset);
			assertTrue(false);
		} catch (Exception e) {
			assertEquals("Could not find tag '#IDA'",e.getMessage());
		}
	}
    
    private InternalSource internalSource(String source) {
        File file = new File(source);
        
        InternalSource internalSource = new InternalSource();
        internalSource.setSource(file.getAbsolutePath());
        internalSource.setTable(dataset.getName());
        internalSource.setSourceSize(file.length());
        return internalSource;
    }

}
