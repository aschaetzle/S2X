package de.tf.uni.freiburg.sparkrdf.parser.rdf;

import java.util.ArrayList;
import java.util.List;

import de.tf.uni.freiburg.sparkrdf.parser.PrefixUtil;

/**
 * Parses a {@link String} to a {@link RDFTuple}
 * 
 * @author Thorsten Berberich
 * 
 */
public class RDFParser extends IRDFParser {

    /**
     * Generated serial UID
     */
    private static final long serialVersionUID = 4398888788474761545L;

    /**
     * Create a new parser with the given prefixes
     * 
     * @param prefixes
     *            Prefix mapping to replace the long forms of the tuple parts
     */
    public RDFParser() {
    }

    @Override
    public String[] parse(String toParse) throws LineMalformedException {
	// Get the text line
	String line = toParse;

	// Remove leading and trailing whitespaces
	line = line.trim();

	final List<String> list = new ArrayList<String>();

	/*
	 * Keeps track if the splitted part of the line is inside a quoted
	 * phrase or not
	 */
	String tmp = "";
	boolean escaped = false;
	for (int pos = 0; pos < line.length(); pos++) {
	    final String charString = line.substring(pos, pos + 1);
	    if (charString.equals("\"")) {
		tmp += charString;
		escaped = !escaped;
	    } else if (charString.matches("\\s")) {
		if (escaped) {
		    tmp += charString;
		} else {
		    if (tmp.length() > 0) {
			list.add(tmp);
			tmp = "";
		    }
		}
	    } else {
		tmp += charString;
	    }
	}
	if (tmp.length() > 0) {
	    list.add(tmp);
	}

	// Wrong arguments
	if (list.size() < 3) {
	    throw new LineMalformedException();
	}

	// Empty argument
	if (list.get(0).isEmpty() || list.get(1).isEmpty()
		|| list.get(2).isEmpty()) {
	    throw new LineMalformedException();
	}

	String objectString = list.get(2);

	// Remove trailing dot
	if (objectString.endsWith(".")) {
	    objectString = objectString.substring(0, objectString.length() - 1);
	}

	// Create the result
	final String[] tuple = new String[3];

	tuple[0] = PrefixUtil.collapsePrefix(list.get(0));
	tuple[1] = PrefixUtil.collapsePrefix(list.get(1));
	tuple[2] = PrefixUtil.collapsePrefix(objectString);

	return tuple;
    }

}
