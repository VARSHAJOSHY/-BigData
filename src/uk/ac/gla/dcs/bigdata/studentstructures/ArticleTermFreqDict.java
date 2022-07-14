package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * Represents term (or word) frequency within the document corpus. This class will be used later for DPH calculation.
 */
public class ArticleTermFreqDict implements Serializable {

	private static final long serialVersionUID = 5779288192985630190L;

	Map<String, Long> termFreqdict; // term and its number of occurences within the corpus

	/**
	 * @param termFreqdict
	 */
	public ArticleTermFreqDict(Map<String, Long> termFreqdict) {
		super();
		this.termFreqdict = termFreqdict;
	}

	/**
	 * @param newDict
	 * @return merged ArticleTermFreqDict merging all terms and frequency (number of
	 *         occurences) within the corpus. ie., article termFrequency reduce to
	 *         corpus termFrequency
	 */
	public ArticleTermFreqDict mergeWith(ArticleTermFreqDict newDict) {
		Map<String, Long> mergedDict = Stream
				.concat(termFreqdict.entrySet().stream(), newDict.getTermFreqdict().entrySet().stream())
				.collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, (value1, value2) -> value1 + value2));
		return new ArticleTermFreqDict(mergedDict);
	}

	/**
	 * @return the termFreqdict
	 */
	public Map<String, Long> getTermFreqdict() {
		return termFreqdict;
	}

	/**
	 * @param termFreqdict the termFreqdict to set
	 */
	public void setTermFreqdict(Map<String, Long> termFreqdict) {
		this.termFreqdict = termFreqdict;
	}

}
