# Objectives

The overall objective of this assessment is to develop a batch-based text search and filtering pipeline in Apache Spark to filter out and rank top 10 news articles from a large dataset on basis on their relevance towards queries from queryset. Below objectives are implemented successfully using spark functions in our application.
* Document pre-processing to remove stopwords , apply stemming and tokenization.
* Scoring the document using DPH ranking model. The DPH score for a <document, query> pair is the average of the DPH scores for each <document, term> pair (for each term in the query).
    * Only those terms within the title and ContentItem elements (of document) that have a non-null subtype and subtype is listed as “paragraph” to be considered for DPH calculation. 
    * In case if news article contains more than 5 paragraphs, only first 5 pargraphs to be considered for DPH calculation.
* Ranking the documents for each query based on DPH score. 
* 	Fiter out near duplicate elements to remove unneeded redundancy 
    * 	Near duplicate documents are having less than 0.5 textual distance between their titles.
    * In such scenario, the most relevant among them based on DPH score to be retrieved.
* Successfully return 10 documents for every query.
* All processing should happen within the lifecycle of the Spark application.

# Dataset
A set of queries and corpus of news articles from the Washington Post are used as dataset. The dataset is divided into two versions.
* A local sample contain 5,000 news articles and 3 queries 
* Full dataset comprised of around 6,70,000 documents and 10 queries.


# Implementation Steps 
1.	Our application takes list of queries and collection of news articles as input and produce top 10 news articles relevant to every query.
2.	List of queries will be loaded from a text file, pre-processed (stopwords removal, tokenization and stemming) by utility functions, and converted into a Dataset<Query> using MapFunction. In similar way, news articles will be loaded from text file and converted to a Dataset<NewsArticle> using MapFunction. In contrast to Dataset<Query>, Dataset<NewsArticle> is an unprocessed dataset.
3.	Since all the data belongs to a news article are not relevant to the given task, we need to retrieve necessary information from news article and create a new class with name NewsArticleInfo to store the extracted information. FlatMapFunction (ArticleInfoFlatMap) is being used to extracts relevant information from an article such as article id, article title and content (an article contains list of contents).
    * First we have filtered out all articles with not null title for further processing.
    * Since terms (or words) within title and contentitem elements that have a non-null subtype and that subtype is listed as “paragraph” are going to use for DPH calculation, retrieve only those values from NewsArticle class and use utility functions (TextPreProcessor class ) to pre-process (stopwords removal, tokenization and stemming) it.
    * Articles may have more than five paragraphs in it. In such scenario, first five must be filtered out before applying pre-processing function.
    * A longAccumulator 'articleLengthAccumulator' is used for calculating the documents length within the corpus which is pass to the MapFunction ArticleInfoMap constructor. Accumulator value will be incremented based on the number of terms present within article title and and contentitem elements that have a non-null subtype and that subtype is listed as “paragraph”.
    * Retrieved terms after text pre-processing will be stored in a dictionary along with its frequency of occurrence. For example, <”America”, 5> ,<”nature”,2>
    * FlatMapFunction will create object of class NewsArticleInfo for every NewsArticle and return the list.
4.	Use a map-reduce function to fetch all terms within the documents in the corpus (Dataset < NewsArticleInfo>) and how many times each terms appears. Create a broadcast variable (Broadcast< ArticleTermFreqDict>) of TermFrequencyDict class object.
5.	To retrieve documents relevant to a particular query, DPH score is being used. The DPH score for a <document, query> pair is the average of the DPH scores for each <document, term> pair (for each term in the query ). A static DPH scoring function will be able to calculate a score for a <document, term> pair. This function takes following I put parameters to generate the score.
    * Number of times the query appears in the document
    * Number of times the query appears in all documents
    * Length of the current document (number of terms in the document)
    * Number of documents in the corpus – size of Dataset< NewsArticle>
    * Average document length in the corpus (in terms) - articleLengthAccumulator (holds document length (in terms) within the corpus) divided by total number of documents within the corpus (ie., size of Dataset< NewsArticle>).

    FlatMapFunction ArtcleDphFlatMap is using for DPH calculation. DPH score of <document, term> will be calculated when flatmap is called for each document. List of Queries (List<Queries>) and term frequency dictionary within the corpus (TermFrequencyDict) will broadcast along with iv and v parameters. The call method will return an iterator with DocumentRanking where DocumentRankingclass represents query and list of documents relevant to the query along with its DPH score.
6.	Call function iterate over Query List to identify whether news article contains query terms or not. DPH score is only calculated if article contains any of the query terms. Then we iterates over the Query list, calculates i, ii, iii and DPH score of a document for every query term will be calculated with DPHScorer.getDPHScore(). We set DPH score as zero, if DPHScorer.getDPHScore() return score value as infinity, -infinity or other than numeric value.  Average of the DPH scores for each <document, term> pair (for each term in the query) will be the DPH score for a <document, query> pair. Call function returns list of DocumentRanking objects where each object represents a query (Query object) and list of RankedResult which stores docid, article and DPH score. 
7.	The returned dataset will be grouped using Query as key, then apply a ReduceGroup function that ranks the articles based on DPH score and merge them together for each query. Inside reduce function, top10 articles from sorted map will be selected and filtered based on the title similarity by using TextDistanceCalculator iteratively and return top 10 documents relevant to each query based on the DPH score.


