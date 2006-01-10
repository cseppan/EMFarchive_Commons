package gov.epa.emissions.commons.io.importer;

import junit.framework.TestCase;

public class FilePatternMatcherTest extends TestCase {

    public void testMatchingNamesWithStarPeriodAndExtension() throws ImporterException {
        String filePattern = "*.txt";
        String[] names = { "cep.txt", "cep0.txt", "cep.exe" };
        FilePatternMatcher patternMatcher = new FilePatternMatcher(filePattern);
        String[] matchingNames = patternMatcher.matchingNames(names);
        assertEquals(2, matchingNames.length);
        assertEquals(names[0], matchingNames[0]);
        assertEquals(names[1], matchingNames[1]);
    }

    public void testMatchingNamesWithStarPeriodStar() throws ImporterException {
        String filePattern = "*.*";
        String[] names = { "cep.txt", "cep0.txt", "cep.exe" };
        FilePatternMatcher patternMatcher = new FilePatternMatcher(filePattern);
        String[] matchingNames = patternMatcher.matchingNames(names);
        assertEquals(3, matchingNames.length);
        assertEquals(names[0], matchingNames[0]);
        assertEquals(names[1], matchingNames[1]);
        assertEquals(names[2], matchingNames[2]);
    }

    public void testMatchingNamesWithStar() throws ImporterException {
        String filePattern = "*";
        String[] names = { "cep.txt", "cep0.txt", "cep.exe" };
        FilePatternMatcher patternMatcher = new FilePatternMatcher(filePattern);
        String[] matchingNames = patternMatcher.matchingNames(names);
        assertEquals(3, matchingNames.length);
        assertEquals(names[0], matchingNames[0]);
        assertEquals(names[1], matchingNames[1]);
        assertEquals(names[2], matchingNames[2]);
    }

    public void testMatchingNamesWithNamePrefixAndStar() throws ImporterException {
        String filePattern = "tes*";
        String[] names = { "test.txt", "tes0.txt", "tes01.exe", "cep.txt", "test02.txt" };
        FilePatternMatcher patternMatcher = new FilePatternMatcher(filePattern);
        String[] matchingNames = patternMatcher.matchingNames(names);
        assertEquals(4, matchingNames.length);
        assertEquals(names[0], matchingNames[0]);
        assertEquals(names[1], matchingNames[1]);
        assertEquals(names[2], matchingNames[2]);
        assertEquals(names[4], matchingNames[3]);
    }

}
