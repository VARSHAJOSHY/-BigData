package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleInfo;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleTermFreqDict;

/*
 * DPH score of <document, term> and average DPH score for a <document, query> pair will be calculated 
 * when FlatMapFunction is called for each document.
 * FlatMapFunction return an iterator of DocumentRanking 
 * where DocumentRanking class represents query and list of documents relevant to the query along with its DPH score.
 */

public class ArtcleDphFlatMap implements FlatMapFunction<NewsArticleInfo, DocumentRanking> {
	/**
	 * 
	 */
	private static final long serialVersionUID = -9209047713513423001L;
	Broadcast<List<Query>> queryList; // List of Queries (List<Queries>)
	Broadcast<ArticleTermFreqDict> corpusTermDict; // term frequency dictionary within the corpus
	long totalDocsInCorpus; // Number of documents in the corpus (size of Dataset< NewsArticle>)
	long averageDocumentLengthInCorpus; // Average document length in the corpus

	/**
	 * @param queryList
	 * @param corpusTermDict
	 * @param totalDocsInCorpus
	 * @param averageDocumentLengthInCorpus
	 */
	public ArtcleDphFlatMap(Broadcast<List<Query>> queryList, Broadcast<ArticleTermFreqDict> corpusTermDict,
			long totalDocsInCorpus, long averageDocumentLengthInCorpus) {
		super();
		this.queryList = queryList;
		this.corpusTermDict = corpusTermDict;
		this.totalDocsInCorpus = totalDocsInCorpus;
		this.averageDocumentLengthInCorpus = averageDocumentLengthInCorpus;
	}

	@Override
	public Iterator<DocumentRanking> call(NewsArticleInfo article) throws Exception {
		long currentDocumentLength = article.getLengthOfDocument();
		long totalTermFrequencyInCorpus = 0;
		long termFrequencyInCurrentDocument = 0;
		double dphValue = 0, avgDphValue = 0.0, queryArticleDph = 0;

		// store dph scores between queries and current article
		List<DocumentRanking> queryDphList = new ArrayList<DocumentRanking>();

		// store dph score for each term, avoiding multiple calculations
		Map<String, Double> termDphMap = new HashMap<String, Double>();

		// fetch term frequency from NewsArticleInfo
		Map<String, Long> termDict = article.getTermDict();

		// iterate over Query List to identify whether news article contains query terms
		// or not
		Iterator<Query> queryIterator = queryList.getValue().parallelStream()
				.filter(query -> !Collections.disjoint(query.getQueryTerms(), termDict.keySet())).iterator();

		while (queryIterator.hasNext()) {
			Query query = queryIterator.next();
			// get all query terms
			List<String> queryTerms = query.getQueryTerms();
			// iterate over query terms and calculate dph score for a <document,term>
			queryArticleDph = 0;
			for (String term : queryTerms) {
				// use term dph score directly if it has been calculated
				if (termDphMap.keySet().contains(term)) {
					queryArticleDph += termDphMap.get(term);
					continue;
				}
				totalTermFrequencyInCorpus = corpusTermDict.getValue().getTermFreqdict().getOrDefault(term, (long) 0);
				termFrequencyInCurrentDocument = termDict.getOrDefault(term, (long) 0);

				dphValue = DPHScorer.getDPHScore((short) termFrequencyInCurrentDocument,
						(int) totalTermFrequencyInCorpus, (int) currentDocumentLength, averageDocumentLengthInCorpus,
						totalDocsInCorpus);
				/*
				 * If DPH score return is infinity, -infinity or other than numeric then set it
				 * as 0
				 */
				if (Double.isNaN(dphValue) || Double.isInfinite(dphValue))
					dphValue = 0.0;

				termDphMap.put(term, dphValue);
				queryArticleDph += dphValue;
			}
			// To calculate DPH score for a <document,query> pair - find the average of the
			// DPH scores for each <document,term> pair
			avgDphValue = queryArticleDph / queryTerms.size();

			List<RankedResult> result = new ArrayList<RankedResult>(1);
			result.add(new RankedResult(article.getId(), article.getArticle(), avgDphValue));

			DocumentRanking articleDphResults = new DocumentRanking(query, result);
			queryDphList.add(articleDphResults);
		}
		return queryDphList.iterator();
	}
}
