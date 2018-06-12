package de.tf.uni.freiburg.sparkrdf.parser;

import java.io.Serializable;

import de.tf.uni.freiburg.sparkrdf.constants.Const;

/**
 * Class that contains methods to collapse prefixes
 * 
 * @author Thorsten Berberich
 * 
 */
public class PrefixUtil implements Serializable {

    /**
     * Generated UID
     */
    private static final long serialVersionUID = -1847405137215586268L;

    /**
     * The predefined prefixes that are supported by the parser.
     */
    private static String[][] prefixes = new String[][] {
	    { "foaf:", "<http://xmlns.com/foaf/0.1/" },
	    { "dc:", "<http://purl.org/dc/elements/1.1/" },
	    { "rdf:", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#" },
	    { "bench:", "<http://localhost/vocabulary/bench/" },
	    { "ub:", "<http://www.lehigh.edu/~zhp2/2004/0401/univ-bench.owl#" },
	    { "bsbm:",
		    "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/" },
	    { "bsbm-inst:",
		    "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/instances/" },
	    { "bsbm-export:",
		    "<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/export/" },
	    { "xsd:", "<http://www.w3.org/2001/XMLSchema#" },

	    { "gr:", "<http://purl.org/goodrelations/" },
	    { "gn:", "<http://www.geonames.org/ontology#" },
	    { "mo:", "<http://purl.org/ontology/mo/" },
	    { "og:", "<http://ogp.me/ns#" },
	    { "rev:", "<http://purl.org/stuff/rev#" },
	    { "sorg:", "<http://schema.org/" },
	    { "wsdbm:", "<http://db.uwaterloo.ca/~galuc/wsdbm/" },

	    { "dcterms:", "<http://purl.org/dc/terms/" },
	    { "dctype:", "<http://purl.org/dc/dcmitype/" },
	    { "rdfs:", "<http://www.w3.org/2000/01/rdf-schema#" },
	    { "swrc:", "<http://swrc.ontoware.org/ontology#" },
	    { "rev:", "<http://purl.org/stuff/rev#" },
	    { "rss:", "<http://purl.org/rss/1.0/" },
	    { "owl:", "<http://www.w3.org/2002/07/owl#" },
	    { "person:", "<http://localhost/persons/" },
	    { "ex:", "<http://example.org/" } };

    /**
     * Replace foaf: and dc: with the specific prefix for WatDiv
     */
    private static void replaceWatDivPrefix() {
	prefixes[0][1] = "<http://xmlns.com/foaf/";
	prefixes[1][1] = "<http://purl.org/dc/terms/";
    }

    /**
     * Collapses predefined leading Prefixes if collapse is set true. If
     * collapse is not set true it just returns the input.
     * 
     * @param input
     * @return input with collapsed Prefix if collapse is set true
     */
    public static String collapsePrefix(String input) {
	if (Const.watDiv()) {
	    replaceWatDivPrefix();
	}

	// check if one of the predefined prefixes matches
	if (input.startsWith("<") && input.endsWith(">")) {
	    for (int i = 0; i < prefixes.length; i++) {
		if (input.contains(prefixes[i][1])) {
		    // replace prefix with abbreviation if a match is found
		    input = input.replace(prefixes[i][1], prefixes[i][0]);
		    // remove closing bracket of URI
		    return input.substring(0, input.length() - 1);
		}
	    }
	}
	return input;
    }
}
