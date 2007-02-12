package gov.epa.emissions.commons.io.importer;

import gov.epa.emissions.commons.Record;
import gov.epa.emissions.commons.io.CustomCharSetInputStreamReader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DelimitedFileReader implements Reader {

    private BufferedReader fileReader;

    private List comments;

    private Tokenizer tokenizer;

    private int lineNumber;

    private String line;

    private String[] inLineComments;

    public DelimitedFileReader(File file, Tokenizer tokenizer) throws FileNotFoundException {
        this(file, new String[] { "#" }, tokenizer);
    }

    public DelimitedFileReader(File file, String[] inLineComments, Tokenizer tokenizer) throws FileNotFoundException {
        // fileReader = new BufferedReader(new FileReader(file));
        try {
            fileReader = new BufferedReader(new CustomCharSetInputStreamReader(new FileInputStream(file)));
        } catch (FileNotFoundException e) {
            throw new FileNotFoundException("File not found.");
        } catch (UnsupportedEncodingException e) {
            throw new FileNotFoundException("Unsupported char set encoding.");
        }

        comments = new ArrayList();
        this.tokenizer = tokenizer;
        this.inLineComments = inLineComments;
        this.lineNumber = 0;
    }

    public void close() throws IOException {
        fileReader.close();
    }

    public Record read() throws IOException, ImporterException {
        String line = fileReader.readLine();

        while (line != null) {
            lineNumber++;
            this.line = line;
            if (isData(line))
                return doRead(line);
            if (isComment(line))
                comments.add(line);

            line = fileReader.readLine();
        }

        return new TerminatorRecord();
    }

    private boolean isData(String line) {
        return !(line.trim().length() == 0) && (!isComment(line));
    }

    private Record doRead(String line) throws ImporterException {
        Record record = new Record();
        if (line.indexOf('!') == -1) {
            String[] tokens = tokenizer.tokens(line);
            record.add(Arrays.asList(tokens));

            return record;
        }

        int inlineCommentPosition = getInlineCommentPosition('!', line);
        String[] tokens = tokenizer.tokens(line.substring(0, inlineCommentPosition));
        record.add(Arrays.asList(tokens));

        if (inlineCommentPosition < line.length())
            record.add(trimInlineComment(line.substring(inlineCommentPosition)));

        return record;

    }

    private int getInlineCommentPosition(char commentChar, String line) {
        int first = line.indexOf(commentChar);

        //to search pattern "xxx!xxx" or 'xxx!xxx'
        Pattern p = Pattern.compile("(\"(.)*[!](.)*\")|('(.)*[!](.)*')");
        Matcher m = p.matcher(line);

        if (!m.find()) {
            return first;
        }

        int end = m.end();
        
        while (m.find()) {
            end = m.end();
        }

        int index = line.substring(end).indexOf(commentChar);

        if (index < 0)
            return line.length();

        return end + index;
    }

    private String trimInlineComment(String comment) {
        // max inline comment width is 128 db column constraint
        // 127= 128-1=> for inline comment char '!'
        if (comment.length() > 127)
            comment = comment.substring(0, 127);
        return comment.startsWith("!") ? comment : "!" + comment;
    }

    private boolean isComment(String line) {
        for (int i = 0; i < inLineComments.length; i++) {
            if (line.startsWith(inLineComments[i]))
                return true;
        }
        return false;
    }

    public List comments() {
        return comments;
    }

    public int lineNumber() {
        return lineNumber;
    }

    public String line() {
        return line;
    }

    // Added to remove header lines
    public String[] readHeader(int numLines) throws IOException {
        List header = new ArrayList();
        for (int i = 0; i < numLines; i++) {
            header.add(fileReader.readLine());
        }

        return (String[]) header.toArray(new String[0]);
    }

    // Added to remove header lines
    public String[] readHeader(String regex) throws IOException {
        List header = new ArrayList();
        String line = fileReader.readLine();

        Pattern pattern = Pattern.compile(regex);
        while (pattern.split(line).length < 3) {
            header.add(line);
            line = fileReader.readLine();
        }

        header.add(line); // Table header - Column Names
        header.add(fileReader.readLine()); // Table header - Units
        header.add(fileReader.readLine()); // FIXME: Assume one more line of table border

        return (String[]) header.toArray(new String[0]);
    }

}
