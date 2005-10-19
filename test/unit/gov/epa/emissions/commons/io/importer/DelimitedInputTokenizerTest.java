package gov.epa.emissions.commons.io.importer;

import junit.framework.TestCase;

public class DelimitedInputTokenizerTest extends TestCase {

    private DelimitedInputTokenizer tokenizer;

    protected void setUp() {
        tokenizer = new DelimitedInputTokenizer();
    }

    public void testTokenizeStringWithSingleQuotedTextContainingSpacesSpaceDelimited() {
        String input = "37119 0001  'REXAMINC.; CUSTOM   DIVISION'   40201301   hi$ya -wE&9 -9 -9 -9 -9";

        String[] tokens = tokenizer.tokensWhitepaceDelimited(input);

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

        String[] tokens = tokenizer.tokensWhitepaceDelimited(input);

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

        String[] tokens = tokenizer.tokensWhitepaceDelimited(input);

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

        String[] tokens = tokenizer.tokensWhitepaceDelimited(input);

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

        String[] tokens = tokenizer.tokensCommaDelimited(input);

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

        String[] tokens = tokenizer.tokensSemiColonDelimited(input);

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

        String[] tokens = tokenizer.tokensWhitepaceDelimited(input);

        assertEquals(3, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("! EPA Derived", tokens[2]);
    }

    public void testTokenizeSpaceDelimitedAutomaticallyBasedOnMiniminExpectedTokens() {
        String input = "37119 0001 ! EPA Derived  ";
        String[] tokens = tokenizer.tokens(input, 3);

        assertEquals(3, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("! EPA Derived", tokens[2]);
    }

    public void testTokenizeCommaDelimitedAutomaticallyBasedOnMiniminExpectedTokens() {
        String input = "37119 , 0001, ! EPA Derived  ";
        String[] tokens = tokenizer.tokens(input, 3);

        assertEquals(3, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("! EPA Derived", tokens[2]);
    }

    public void testTokenizeSemiColonDelimitedAutomaticallyBasedOnMiniminExpectedTokens() {
        String input = "37119 ; 0001 ;! EPA Derived  ";
        String[] tokens = tokenizer.tokensSemiColonDelimited(input);

        assertEquals(3, tokens.length);
        assertEquals("37119", tokens[0]);
        assertEquals("0001", tokens[1]);
        assertEquals("! EPA Derived", tokens[2]);
    }
}
