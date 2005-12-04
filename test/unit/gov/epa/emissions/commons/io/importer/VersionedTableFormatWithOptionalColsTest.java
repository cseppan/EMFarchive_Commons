package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.db.SqlDataTypes;
import gov.epa.emissions.commons.io.Column;
import gov.epa.emissions.commons.io.FileFormatWithOptionalCols;

import java.util.ArrayList;
import java.util.List;

import org.jmock.Mock;
import org.jmock.MockObjectTestCase;

public class VersionedTableFormatWithOptionalColsTest extends MockObjectTestCase {

    public void testRecordIdShouldBeTheKey() {
        Mock types = mock(SqlDataTypes.class);
        types.stubs().method(ANYTHING).will(returnValue(""));

        Mock fileFormat = mock(FileFormatWithOptionalCols.class);
        fileFormat.stubs().method("cols").will(returnValue(new Column[0]));

        VersionedTableFormatWithOptionalCols format = new VersionedTableFormatWithOptionalCols(
                (FileFormatWithOptionalCols) fileFormat.proxy(), (SqlDataTypes) types.proxy());

        assertEquals("Record_Id", format.key());
    }

    public void testShouldHaveFourFixedVersionRelatedColumns() {
        Mock types = mock(SqlDataTypes.class);
        types.stubs().method("longType").will(returnValue("long"));
        types.stubs().method("stringType").withAnyArguments().will(returnValue("long"));
        types.stubs().method("autoIncrement").withAnyArguments().will(returnValue("serial"));
        types.stubs().method("text").withAnyArguments().will(returnValue("text"));

        Mock fileFormat = mock(FileFormatWithOptionalCols.class);
        fileFormat.stubs().method("cols").will(returnValue(new Column[0]));

        VersionedTableFormatWithOptionalCols format = new VersionedTableFormatWithOptionalCols(
                (FileFormatWithOptionalCols) fileFormat.proxy(), (SqlDataTypes) types.proxy());

        Column[] cols = format.cols();

        assertEquals(5, cols.length);
        // version-related cols
        assertEquals("Record_Id", cols[0].name());
        assertEquals("Dataset_Id", cols[1].name());
        assertEquals("Version", cols[2].name());
        assertEquals("Delete_Versions", cols[3].name());

        assertEquals("Comments", cols[4].name());
    }

    public void testShouldPlaceDataColsBetweenVersionColumnsAndCommentsCols() {
        Mock types = mock(SqlDataTypes.class);
        types.stubs().method(ANYTHING).will(returnValue(""));

        Mock fileFormat = mock(FileFormatWithOptionalCols.class);
        Column dataCol = new Column("data", "type");
        fileFormat.stubs().method("cols").will(returnValue(new Column[] { dataCol }));

        VersionedTableFormatWithOptionalCols format = new VersionedTableFormatWithOptionalCols(
                (FileFormatWithOptionalCols) fileFormat.proxy(), (SqlDataTypes) types.proxy());

        Column[] cols = format.cols();

        assertEquals(6, cols.length);

        // version-related cols
        assertEquals("Record_Id", cols[0].name());
        assertEquals("Dataset_Id", cols[1].name());
        assertEquals("Version", cols[2].name());
        assertEquals("Delete_Versions", cols[3].name());

        assertSame(dataCol, cols[4]);

        assertEquals("Comments", cols[5].name());
    }

    public void testShouldAddVersionDataDefaultForOptionalColsAndEmptyCommentsOnFillUsingDataWithPartialOptionalDataAndNoComments() {
        Mock types = mock(SqlDataTypes.class);
        types.stubs().method(ANYTHING).will(returnValue(""));

        Mock fileFormat = mock(FileFormatWithOptionalCols.class);
        fileFormat.stubs().method("cols").will(returnValue(new Column[0]));

        Column optional1 = new Column("data", "type");
        Column optional2 = new Column("data", "type");
        fileFormat.stubs().method("optionalCols").will(returnValue(new Column[] { optional1, optional2 }));
        fileFormat.stubs().method("minCols").will(returnValue(new Column[0]));

        VersionedTableFormatWithOptionalCols format = new VersionedTableFormatWithOptionalCols(
                (FileFormatWithOptionalCols) fileFormat.proxy(), (SqlDataTypes) types.proxy());

        List data = new ArrayList();
        data.add("opt1");// 1 of 2 filled in
        long datasetId = 129;

        format.fill(data, datasetId);

        // 4 - version cols, 2 optionals, 1 Comments
        assertEquals((4 + 2 + 1), data.size());
        assertEquals("", data.get(0));// record id
        assertEquals(datasetId + "", data.get(1));
        assertEquals("0", data.get(2)); // version
        assertEquals("", data.get(3));// delete versions
        assertEquals("opt1", data.get(4));// optional 1
        assertEquals("", data.get(5));// optional 2
        assertEquals("", data.get(6));// comments
    }

    public void testShouldAddVersionDataDefaultForOptionalColsAndEmptyCommentsOnFillUsingDataWithNoOptionalDataAndComments() {
        Mock types = mock(SqlDataTypes.class);
        types.stubs().method(ANYTHING).will(returnValue(""));

        Mock fileFormat = mock(FileFormatWithOptionalCols.class);
        fileFormat.stubs().method("cols").will(returnValue(new Column[0]));

        Column optional1 = new Column("data", "type");
        Column optional2 = new Column("data", "type");
        fileFormat.stubs().method("optionalCols").will(returnValue(new Column[] { optional1, optional2 }));
        fileFormat.stubs().method("minCols").will(returnValue(new Column[0]));

        VersionedTableFormatWithOptionalCols format = new VersionedTableFormatWithOptionalCols(
                (FileFormatWithOptionalCols) fileFormat.proxy(), (SqlDataTypes) types.proxy());

        List data = new ArrayList();
        data.add("!Comments");// comments
        long datasetId = 129;

        format.fill(data, datasetId);

        // 4 - version cols, 2 optionals, 1 Comments
        assertEquals((4 + 2 + 1), data.size());
        assertEquals("", data.get(0));// record id
        assertEquals(datasetId + "", data.get(1));
        assertEquals("0", data.get(2)); // version
        assertEquals("", data.get(3));// delete versions
        assertEquals("", data.get(4));// optional 1
        assertEquals("", data.get(5));// optional 2
        assertEquals("!Comments", data.get(6));// comments
    }
}
