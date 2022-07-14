package uk.ac.gla.dcs.bigdata.apps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;

import scala.Tuple2;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormaterMap;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentfunctions.ArtcleDphFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.ArticleInfoFlatMap;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocumentRankingReducer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsArticleInfo;
import uk.ac.gla.dcs.bigdata.studentstructures.ArticleTermFreqDict;

/**
 * This is the main class where your Spark topology should be specified.
 * 
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by the
 * spark.master environment variable.
 * 
 * @author Richard
 *
 */
public class AssessedExercise {

	public static void main(String[] args) {

		long start = System.currentTimeMillis();

		// The code submitted for the assessed exerise may be run in either local or
		// remote modes
		// Configuration of this will be performed based on an environment variable
		String sparkMasterDef = System.getenv("SPARK_MASTER");
		if (sparkMasterDef == null) {
			File hadoopDIR = new File("resources/hadoop/"); // represent the hadoop directory as a Java file so we can
															// get an absolute path for it
			System.setProperty("hadoop.home.dir", hadoopDIR.getAbsolutePath()); // set the JVM system property so that
																				// Spark finds it
			sparkMasterDef = "local[2]"; // default is local mode with two executors
		}

		String sparkSessionName = "BigDataAE"; // give the session a name

		// Create the Spark Configuration
		SparkConf conf = new SparkConf().setMaster(sparkMasterDef).setAppName(sparkSessionName);

		// Create the spark session
		SparkSession spark = SparkSession.builder().config(conf).getOrCreate();

		// Get the location of the input queries
		String queryFile = System.getenv("BIGDATA_QUERIES");
		if (queryFile == null)
			queryFile = "data/queries.list"; // default is a sample with 3 queries

		// Get the location of the input news articles
		String newsFile = System.getenv("BIGDATA_NEWS");
		if (newsFile == null)
			newsFile = "data/TREC_Washington_Post_collection.v3.example.json"; // default is a sample of 5000 news
																				// articles

		// Call the student's code
		List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

		// Close the spark session
		spark.close();

		String out = System.getenv("BIGDATA_RESULTS");
		String resultsDIR = "results/";
		if (out != null)
			resultsDIR = out;

		// Check if the code returned any results
		if (results == null)
			System.err
					.println("Topology return no rankings, student code may not be implemented, skiping final write.");
		else {

			// Write the ranking for each query as a new file
			for (DocumentRanking rankingForQuery : results) {
				rankingForQuery.write(new File(resultsDIR).getAbsolutePath());
			}
		}

		try {
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(
					new FileOutputStream(new File(resultsDIR).getAbsolutePath() + "/SPARK.DONE")));
			writer.write(String.valueOf(System.currentTimeMillis()));
			writer.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		long end = System.currentTimeMillis();
		long elapsedTime = end - start;
		System.out.println("Execution Time Taken :" + elapsedTime);
	}

	public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

		// Load queries and news articles
		Dataset<Row> queriesjson = spark.read().text(queryFile);
		Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article

		// Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java
		// objects

		// this converts each row into a Query
		Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(), Encoders.bean(Query.class)); //

		// this converts each row into a NewsArticle
		Dataset<NewsArticle> news = newsjson.map(new NewsFormaterMap(), Encoders.bean(NewsArticle.class));

		// ----------------------------------------------------------------
		// Your Spark Topology should be defined here
		// ----------------------------------------------------------------

		// Created accumulator for calculating article length within the corpus in
		// number of terms
		LongAccumulator articleLengthAccumulator = spark.sparkContext().longAccumulator();

		// Filter null title article and extract useful information from remain
		Dataset<NewsArticleInfo> articleInfo = news.flatMap(new ArticleInfoFlatMap(articleLengthAccumulator),
				Encoders.bean(NewsArticleInfo.class));
		
		// Number of documents in the corpus
		long totalDocsInCorpus = articleInfo.count();
		// The average document length in the corpus (in terms). 
		// Get articleLengthAccumulator value correctly
		long averageDocumentLengthInCorpus = articleLengthAccumulator.value() / totalDocsInCorpus;

		// article term frequency within the corpus
		ArticleTermFreqDict corpusTermDict = articleInfo
				// get termFrequency dictionary for each article, wrapping Map<String, Long> in ArticleTermFreqDict class
				.map((MapFunction<NewsArticleInfo, ArticleTermFreqDict>)(article -> new ArticleTermFreqDict(article.getTermDict())), Encoders.bean(ArticleTermFreqDict.class))
				// merge all termFrequency dictionaries together which returns term-frequency of all articles within the corpus
				.reduce((ReduceFunction<ArticleTermFreqDict>)((dict1, dict2) -> dict1.mergeWith(dict2)));

		// sending ArticleTermFreqDict to ArtcleDphFlatMap function efficiently through
		// broadcasting
		Broadcast<ArticleTermFreqDict> broadcastTermDict = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(corpusTermDict);

		// convert Dataset<Query> into List<Query>
		List<Query> queryList = queries.collectAsList();

		// sending list of queries into ArtcleDphFlatMap function efficiently through
		// broadcasting
		Broadcast<List<Query>> broadcastQuery = JavaSparkContext.fromSparkContext(spark.sparkContext())
				.broadcast(queryList);

		// Got query result
		List<DocumentRanking> results = articleInfo
				// calculate DPH value for query-article pair: Dataset<DocumentRanking>
				.flatMap(new ArtcleDphFlatMap(broadcastQuery, broadcastTermDict, totalDocsInCorpus,
						averageDocumentLengthInCorpus), Encoders.bean(DocumentRanking.class))
				// group by query: KeyValueGroupedDataset<Query, DocumentRanking>
				.groupByKey((MapFunction<DocumentRanking, Query>)(ranking -> ranking.getQuery()), Encoders.bean(Query.class))
				// ranking article by dph and merging rankings together for each query: Dataset<Tuple2<Query, DocumentRanking>>
				.reduceGroups(new DocumentRankingReducer())
				// map to DocumentRanking: Dataset<DocumentRanking>
				.map((MapFunction<Tuple2<Query, DocumentRanking>, DocumentRanking>)(tuple -> tuple._2), Encoders.bean(DocumentRanking.class))
				// transform to list: List<DocumentRanking>
				.collectAsList();

		for (DocumentRanking result : results) {
			System.out.println(result);
		}
		return results;
	}
}
