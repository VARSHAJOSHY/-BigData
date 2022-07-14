package uk.ac.gla.dcs.bigdata.providedutilities;

import org.apache.commons.text.similarity.LevenshteinDistance;

import com.google.common.base.Strings;


/**
 * Calculates the distance between two strings using LevenshteinDistance
 * normalised into a 0-1 range. Higher values indicate the strings are more
 * dissimilar.
 * @author Richard
 *
 */
public class TextDistanceCalculator {

	/**
	 * Calculate the normalised distance between two strings. Outputs a value
	 * between 0 and 1, where higher values indicate the texts are more dissimilar
	 * @param textSnippet1 - input text 1
	 * @param textSnippet2 - input text 2
	 * @return
	 */
	public static double similarity(String textSnippet1, String textSnippet2) {
		
		//to handle textual calculation of news article with title is null
		if (Strings.isNullOrEmpty(textSnippet1)) textSnippet1=" ";
		if (Strings.isNullOrEmpty(textSnippet2)) textSnippet2=" ";
		
		LevenshteinDistance ld = new LevenshteinDistance(); // apache.commons.text implementation of LevenshteinDistance
		int editDistance = ld.apply(textSnippet1, textSnippet2); // calculate the edit distance
		
		// get the maximum length of the inputs for normalization
		int maxCharLength = textSnippet1.length(); 
		if (textSnippet2.length()>textSnippet1.length()) maxCharLength = textSnippet2.length();
		
		// normalize and return the distance
		return (1.0*editDistance)/maxCharLength;
		
	}
	
}
