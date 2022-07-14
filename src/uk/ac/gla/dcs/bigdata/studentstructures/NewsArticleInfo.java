package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.Map;

import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;

/*
 * This class is created from base class NewsArticle.  Class represents a single article from Washington Post contains essential information needed to 
 * achieve the task mainly id, title, and content fields of a document. In addition to this, class hold variables to store length of the document 
 * and term (or word) frequency within the document.
 */

public class NewsArticleInfo implements Serializable {

	private static final long serialVersionUID = -1942320184372541876L;

	String id; // unique article identifier
	String title; // article title
	NewsArticle article; // Article
	Map<String, Long> termDict; // term frequency occurrences
	long lengthOfDocument; // length of article in number of terms

	// paramterised constructor
	public NewsArticleInfo(String id, String title, NewsArticle article, Map<String, Long> termDict,
			long lengthOfDocument) {
		super();
		this.id = id;
		this.title = title;
		this.article = article;
		this.termDict = termDict;
		this.lengthOfDocument = lengthOfDocument;
	}

	/**
	 * @return the id
	 */
	public String getId() {
		return id;
	}

	/**
	 * @param id the id to set
	 */
	public void setId(String id) {
		this.id = id;
	}

	/**
	 * @return the title
	 */
	public String getTitle() {
		return title;
	}

	/**
	 * @param title the title to set
	 */
	public void setTitle(String title) {
		this.title = title;
	}

	/**
	 * @return the article
	 */
	public NewsArticle getArticle() {
		return article;
	}

	/**
	 * @param article the article to set
	 */
	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	/**
	 * @return the termDict
	 */
	public Map<String, Long> getTermDict() {
		return termDict;
	}

	/**
	 * @param termDict the termDict to set
	 */
	public void setTermDict(Map<String, Long> termDict) {
		this.termDict = termDict;
	}

	/**
	 * @return the lengthOfDocument
	 */
	public long getLengthOfDocument() {
		return lengthOfDocument;
	}

	/**
	 * @param lengthOfDocument the lengthOfDocument to set
	 */
	public void setLengthOfDocument(long lengthOfDocument) {
		this.lengthOfDocument = lengthOfDocument;
	}

}
