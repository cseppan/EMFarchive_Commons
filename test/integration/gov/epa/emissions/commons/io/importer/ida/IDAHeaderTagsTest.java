package gov.epa.emissions.commons.io.importer.ida;

import gov.epa.emissions.commons.db.Datasource;
import gov.epa.emissions.commons.db.DbServer;
import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.SimpleDataset;
import gov.epa.emissions.commons.io.importer.FileFormat;
import gov.epa.emissions.commons.io.importer.DbTestCase;
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

	protected void tearDown() throws Exception {
		DbUpdate dbUpdate = new DbUpdate(datasource.getConnection());
		dbUpdate.dropTable(datasource.getName(), dataset.getName());
	}

	public void testShouldIdentifyAllRequiredTags() throws Exception {
		File file = new File("test/data/ida/small-area.txt");
		BufferedReader reader = new BufferedReader(new FileReader(file));
		IDAHeaderReader headerReader = new IDAHeaderReader(reader);
		headerReader.read();

		FileFormat colsMetadata = new IDAAreaFileFormat(headerReader
				.polluntants(), sqlDataTypes);
		IDAImporter importer = new IDAImporter(datasource, sqlDataTypes);
		importer.run(reader, colsMetadata, headerReader.comments(), dataset);
	}

	public void testShouldIdentifyNoIDATag() throws Exception {
		File file = new File("test/data/ida/noIDATags.txt");
		BufferedReader reader = new BufferedReader(new FileReader(file));
		IDAHeaderReader headerReader = new IDAHeaderReader(reader);
		headerReader.read();

		FileFormat colsMetadata = new IDAAreaFileFormat(headerReader
				.polluntants(), sqlDataTypes);
		IDAImporter importer = new IDAImporter(datasource, sqlDataTypes);
		try {
			importer
					.run(reader, colsMetadata, headerReader.comments(), dataset);
			assertTrue(false);
		} catch (Exception e) {
			assertEquals("Could not find tag '#IDA'",e.getMessage());
		}
	}

}
