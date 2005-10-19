package gov.epa.emissions.commons.io.importer;

import junit.framework.TestCase;

public class DelimitedInputTokenizerTest extends TestCase {

    public void testTokenizeStringWithSingleQuotedTextContainingSpacesSpaceDelimited() {
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

    public void testTokenizeStringWithDoubleQuotedTextContainingSpacesSpaceDelimited() {
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

    public void testTokenizeStringWithDoubleQuotedTextContainingSpacesAndSingleQuotesSpaceDelimited() {
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

    public void testTokenizeStringWithDoubleQuotedTextContainingSpacesAndSingleQuotesTabDelimited() {
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

    public void testTokenizeStringWithDoubleQuotedTextContainingSpacesAndSingleQuotesCommaDelimited() {
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

    public void testTokenizeStringWithDoubleQuotedTextContainingSpacesAndSingleQuotesSemiColonDelimited() {
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

    public void testTokenizeTrailingInlineComments() {
        String input = "37119 0001 ! EPA Derived  ";

        WhitespaceDelimitedTokenizer tokenizer = new WhitespaceDelimitedTokenizer();
        String[] tokens = tokenizer.tokens(input);

        assertEquals(3, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("! EPA Derived", tokens[2]);
    }

    public void testTokenizeSpaceDelimitedAutomaticallyBasedOnMiniminExpectedTokens() {
        String input = "37119 0001 ! EPA Derived  ";
        DelimiterIdentifyingTokenizer tokenizer = new DelimiterIdentifyingTokenizer(3);
        String[] tokens = tokenizer.tokens(input);

        assertEquals(3, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("! EPA Derived", tokens[2]);
    }

    public void testTokenizeCommaDelimitedAutomaticallyBasedOnMiniminExpectedTokens() {
        String input = "37119 , 0001, ! EPA Derived  ";
        DelimiterIdentifyingTokenizer tokenizer = new DelimiterIdentifyingTokenizer(3);
        String[] tokens = tokenizer.tokens(input);

        assertEquals(3, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("! EPA Derived", tokens[2]);
    }

    public void testTokenizeSemiColonDelimitedAutomaticallyBasedOnMiniminExpectedTokens() {
        String input = "37119 ; 0001 ;! EPA Derived  ";
        Tokenizer tokenizer = new SemiColonDelimitedTokenizer();
        String[] tokens = tokenizer.tokens(input);

        assertEquals(3, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("! EPA Derived", tokens[2]);
    }
}
