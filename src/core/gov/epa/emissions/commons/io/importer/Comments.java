package gov.epa.emissions.commons.io.importer;

import java.util.Iterator;
import java.util.List;

public class Comments {

    private List comments;

    public Comments(List comments) {
        this.comments = comments;
    }

    /**
     * Do not specify '#'. Implicit.
     */
    public String content(String tag) {
        tag = "#" + tag;
        for (Iterator iter = comments.iterator(); iter.hasNext();) {
            String comment = (String) iter.next();
            if (comment.startsWith(tag)) {
                return comment.substring(tag.length()).trim();
            }
        }

        return null;
    }

    /**
     * Have a comment starting with 'tag'.
     * 
     * @param tag
     *            Do not specify '#'. Implicit.
     */
    public boolean have(String tag) {
        return content(tag) != null;
    }

    public String all() {
        StringBuffer description = new StringBuffer();
        for (Iterator iter = comments.iterator(); iter.hasNext();)
            description.append(iter.next() + "\n");

        return description.toString();
    }

    /**
     * Do not specify '#'. Implicit.
     */
    public boolean hasContent(String tag) {
        String comment = content(tag);
        return comment != null && comment.length() > 0;
    }

}
