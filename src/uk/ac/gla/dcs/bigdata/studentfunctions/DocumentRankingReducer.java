package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;

import org.apache.spark.api.java.function.ReduceFunction;

import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;

/*
 * ReduceGroup function takes document list relevant to the query, removing unneeded redundancy by using the provided comparison 
 * function and rank the top 10 most relevant articles by comparing their DPH score.
 */
public class DocumentRankingReducer implements ReduceFunction<DocumentRanking>   {
	private static final long serialVersionUID = -6733172008502653353L;

	@Override
	public DocumentRanking call(DocumentRanking v1, DocumentRanking v2) throws Exception {
		
		// Using LinkedHashMap to store the dph-sorted articles 
		Iterator<RankedResult> allResults = Stream.concat(v1.getResults().stream(), v2.getResults().stream())
				.sorted(Comparator.comparing(RankedResult::getScore).reversed()) 
				.iterator();
		
		// Select the top10 articles from sorted map and filter the redundancy by using TextDistanceCalculator
		List<RankedResult> top10results = new ArrayList<RankedResult>(10);
		while (allResults.hasNext()) {
			RankedResult current = allResults.next();
			if (top10results.size() < 10 && top10results.parallelStream().allMatch(result -> TextDistanceCalculator.similarity(result.getArticle().getTitle(), current.getArticle().getTitle()) >= 0.5)) {
				top10results.add(current);
			}
		}
		v1.setResults(top10results);;
		return v1;
	}
}
