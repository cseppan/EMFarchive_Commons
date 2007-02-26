package gov.epa.emissions.commons.io.importer;

import junit.framework.TestCase;

public class DelimitedInputTokenizerTest extends TestCase {

    public void testTokenizeStringWithSingleQuotedTextContainingSpacesSpaceDelimited() throws ImporterException {
        String input = "37119 0001  'REXAMINC.; CUSTOM   DIVISION'   40201301   hi$ya -wE&9 -9 -9 -9 -9";

        WhitespaceDelimitedTokenizer tokenizer = new WhitespaceDelimitedTokenizer();
        String[] tokens = tokenizer.tokens(input);

        assertEquals(10, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("REXAMINC.; CUSTOM   DIVISION", tokens[2]);
        assertEquals("40201301", tokens[3]);
        assertEquals("hi$ya", tokens[4]);
        assertEquals("-wE&9", tokens[5]);
        assertEquals("-9", tokens[9]);
    }

    public void testTokenizeStringWithDoubleQuotedTextContainingSpacesSpaceDelimited() throws ImporterException {
        String input = "37119 0001  \"REXAMINC.; CUSTOM   DIVISION\"   40201301   hi$ya -wE&9 -9 -9 -9 -9";

        WhitespaceDelimitedTokenizer tokenizer = new WhitespaceDelimitedTokenizer();
        String[] tokens = tokenizer.tokens(input);

        assertEquals(10, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("REXAMINC.; CUSTOM   DIVISION", tokens[2]);
        assertEquals("40201301", tokens[3]);
        assertEquals("hi$ya", tokens[4]);
        assertEquals("-wE&9", tokens[5]);
        assertEquals("-9", tokens[9]);
    }

    public void testTokenizeStringWithDoubleQuotedTextContainingSpacesAndSingleQuotesSpaceDelimited() throws ImporterException {
        String input = "37119 0001  \"REXAMINC.; CUSTOM'S   DIVISION\"   40201301   hi$ya -wE&9 -9 -9 -9 -9";

        WhitespaceDelimitedTokenizer tokenizer = new WhitespaceDelimitedTokenizer();
        String[] tokens = tokenizer.tokens(input);

        assertEquals(10, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("REXAMINC.; CUSTOM'S   DIVISION", tokens[2]);
        assertEquals("40201301", tokens[3]);
        assertEquals("hi$ya", tokens[4]);
        assertEquals("-wE&9", tokens[5]);
        assertEquals("-9", tokens[9]);
    }

    public void testTokenizeStringWithDoubleQuotedTextContainingSpacesAndSingleQuotesTabDelimited() throws ImporterException {
        String input = "37119   0001  \"REXAMINC.; CUSTOM'S     DIVISION\"      40201301    hi$ya   -wE&9   -9  -9  -9  -9";

        WhitespaceDelimitedTokenizer tokenizer = new WhitespaceDelimitedTokenizer();
        String[] tokens = tokenizer.tokens(input);

        assertEquals(10, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("REXAMINC.; CUSTOM'S     DIVISION", tokens[2]);
        assertEquals("40201301", tokens[3]);
        assertEquals("hi$ya", tokens[4]);
        assertEquals("-wE&9", tokens[5]);
        assertEquals("-9", tokens[9]);
    }

    public void testTokenizeStringWithDoubleQuotedTextContainingSpacesAndSingleQuotesCommaDelimited() throws ImporterException {
        String input = "37119, 0001, \"REXAMINC.; CUSTOM'S   DIVISION, IMMIGRATION\" , 40201301 ,hi$ya,-wE&9,-9,-9,-9,-9";

        CommaDelimitedTokenizer tokenizer = new CommaDelimitedTokenizer();
        String[] tokens = tokenizer.tokens(input);

        assertEquals(10, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("REXAMINC.; CUSTOM'S   DIVISION, IMMIGRATION", tokens[2]);
        assertEquals("40201301", tokens[3]);
        assertEquals("hi$ya", tokens[4]);
        assertEquals("-wE&9", tokens[5]);
        assertEquals("-9", tokens[9]);
    }

    public void testTokenizeStringWithDoubleQuotedTextContainingSpacesAndSingleQuotesSemiColonDelimited() throws ImporterException {
        String input = "37119; 0001; \"REXAMINC. ; CUSTOM'S   DIVISION, IMMIGRATION\"; 40201301 ; hi$ya;-wE&9;-9;-9;-9;-9";

        Tokenizer tokenizer = new SemiColonDelimitedTokenizer();
        String[] tokens = tokenizer.tokens(input);

        assertEquals(10, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("REXAMINC. ; CUSTOM'S   DIVISION, IMMIGRATION", tokens[2]);
        assertEquals("40201301", tokens[3]);
        assertEquals("hi$ya", tokens[4]);
        assertEquals("-wE&9", tokens[5]);
        assertEquals("-9", tokens[9]);
    }

    public void testTokenizeCommaDelimitedAutomaticallyBasedOnMiniminExpectedTokens() throws ImporterException {
        String input = "37119 , 0001, ! EPA Derived  ";
        DelimiterIdentifyingTokenizer tokenizer = new DelimiterIdentifyingTokenizer(3);
        String[] tokens = tokenizer.tokens(input);

        assertEquals(3, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("! EPA Derived", tokens[2]);
    }

    public void testTokenizeSemiColonDelimitedAutomaticallyBasedOnMiniminExpectedTokens() throws ImporterException {
        String input = "37119 ; 0001 ;! EPA Derived  ";
        Tokenizer tokenizer = new SemiColonDelimitedTokenizer();
        String[] tokens = tokenizer.tokens(input);

        assertEquals(3, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("! EPA Derived", tokens[2]);
    }

    public void testTokenizeWithTwoSingleQuotedStrings() throws ImporterException {
        String input = "37001 'ES1801f1207' 1 1 1 'Roche Biomedical' 2601020000";
        Tokenizer tokenizer = new WhitespaceDelimitedTokenizer();
        String[] tokens = tokenizer.tokens(input);
        
        assertEquals(7, tokens.length);
        assertEquals("37001", tokens[0]);
        assertEquals("ES1801f1207", tokens[1]);
        assertEquals("1", tokens[2]);
        assertEquals("1", tokens[3]);
        assertEquals("1", tokens[4]);
        assertEquals("Roche Biomedical", tokens[5]);
    }

    public void testTokenizeWithEmptyTokens() throws ImporterException {
        Tokenizer tokenizer = new CommaDelimitedTokenizer();
       
        String input = "37001,,5, 15";
        String[] tokens = tokenizer.tokens(input);
        assertEquals(4, tokens.length);
        assertEquals("37001", tokens[0]);
        assertEquals("", tokens[1]);
        assertEquals("5", tokens[2]);
        assertEquals("15", tokens[3]);
        
        input = "37001, ,,5, 15";
        tokens = tokenizer.tokens(input);
        assertEquals(5, tokens.length);
        assertEquals("37001", tokens[0]);
        assertEquals("", tokens[1]);
        assertEquals("", tokens[2]);
        assertEquals("5", tokens[3]);
        assertEquals("15", tokens[4]);
        
        input = "37001, ,,5, 15,";
        tokens = tokenizer.tokens(input);
        
        assertEquals(6, tokens.length);
        assertEquals("37001", tokens[0]);
        assertEquals("", tokens[1]);
        assertEquals("", tokens[2]);
        assertEquals("5", tokens[3]);
        assertEquals("15", tokens[4]);
        assertEquals("", tokens[5]);
        
        input = ",,37001, , ,5, 15,,,,,";
        tokens = tokenizer.tokens(input);
        assertEquals(12, tokens.length);
        assertEquals("", tokens[0]);
        assertEquals("", tokens[1]);
        assertEquals("37001", tokens[2]);
        assertEquals("", tokens[3]);
        assertEquals("", tokens[4]);
        assertEquals("5", tokens[5]);
        assertEquals("15", tokens[6]);
        assertEquals("", tokens[7]);

    }
    
    public void testLineStartsWithTheDoubleQuoteForDelimitedIdenitifyingTokenizer() throws ImporterException {
        String input = "\"00,0,0\";\"ACROLEI_NOI\";\"ACROLEIN\";1;56.0633;1";
        Tokenizer tokenizer = new DelimiterIdentifyingTokenizer(3);
        String[] tokens = tokenizer.tokens(input);
        
        assertEquals(6, tokens.length);
        assertEquals("00,0,0", tokens[0]);
        assertEquals("ACROLEI_NOI", tokens[1]);
        assertEquals("ACROLEIN", tokens[2]);
        assertEquals("1", tokens[3]);
        assertEquals("56.0633", tokens[4]);
        assertEquals("1", tokens[5]);
    }
    
}
