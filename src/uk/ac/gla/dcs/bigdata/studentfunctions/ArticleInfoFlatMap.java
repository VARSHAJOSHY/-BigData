package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.util.LongAccumulator;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedutilities.TextPreProcessor;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleInfo;

//MapFunction which retrieve essential information (mainly id, title, and content fields of a document) needed to fulfil the task 
//from every NewsArticle and return NewsArticleInfo object
//filter out first five paragraphs from an article with content that has a non-null subtype and subtype is listed as �paragraph�. 
//Pre-process(stopwords removal, tokenization and stemming) the filtered data using utility methods. 

public class ArticleInfoFlatMap implements FlatMapFunction<NewsArticle, NewsArticleInfo> {

	private static final long serialVersionUID = 3298702966660325281L;

	private transient TextPreProcessor processor;
	LongAccumulator articleLengthAccumulator;

	public ArticleInfoFlatMap(LongAccumulator articleLengthAccumulator) {
		super();
		this.articleLengthAccumulator = articleLengthAccumulator;
	}

	@Override
	public Iterator<NewsArticleInfo> call(NewsArticle value) throws Exception {
		List<String> terms;

		if (processor == null)
			processor = new TextPreProcessor();

		String title = value.getTitle();
		
		// check whether article title is null or not. If title is null, exclude the article.
		if (title == null) {
			return Collections.emptyIterator();
		}

		if (value.getContents() == null) {
			terms = new ArrayList<String>();
		} else {
			// fetch first 5 paragraph terms with content that has a non-null subtype and
			// subtype is listed as 'paragraph' and pre-process it
			terms = value.getContents().stream()
					.filter(content -> (content != null) && (content.getSubtype() != null)
							&& (content.getSubtype().equalsIgnoreCase("paragraph")))
					.limit(5).flatMap(content -> processor.process(content.getContent()).stream())
					.collect(Collectors.toList());
		}

		// preprocess article title and add terms within the title
		terms.addAll(processor.process(title));
		
		// length of document in number of terms
		Integer lengthOfDocument = terms.size();

		// accumulate total number of terms after pre-processing the title and first 5
		// paragraphs
		articleLengthAccumulator.add(lengthOfDocument);

		// term dictionary
		Map<String, Long> termDict = new HashMap<String, Long>();
		for (String term : terms) {
			termDict.put(term, termDict.getOrDefault(term, (long) 0) + 1);
		}
		
		List<NewsArticleInfo> infoList = new ArrayList<NewsArticleInfo>(1);
		infoList.add(new NewsArticleInfo(value.getId(), title, value, termDict, lengthOfDocument));
		return infoList.iterator();
	}
}