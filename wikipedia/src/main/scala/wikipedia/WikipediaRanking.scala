package wikipedia

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import org.apache.spark.rdd.RDD

case class WikipediaArticle(title: String, text: String) {
  /**
    * @return Whether the text of this article mentions `lang` or not
    * @param lang Language to look for (e.g. "Scala")
    */
  def mentionsLanguage(lang: String): Boolean = text.toLowerCase().split(' ').contains(lang.toLowerCase())
}

object WikipediaRanking {

  val langs = List(
    "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
    "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

  val conf: SparkConf = new SparkConf()
    .setMaster("local[*]")
    .setAppName("WikipediaApp")
    .set("spark.executor.memory", "1g")

  val sc: SparkContext = new SparkContext(conf)

  // Hint: use a combination of `sc.textFile`, `WikipediaData.filePath` and `WikipediaData.parse`

  val wikiRdd: RDD[WikipediaArticle] = sc.textFile(WikipediaData.filePath).map(article => WikipediaData.parse(article)).cache()

  /** Returns the number of articles on which the language `lang` occurs.
    * Hint1: consider using method `aggregate` on RDD[T].
    * Hint2: consider using method `mentionsLanguage` on `WikipediaArticle`
    *
    * Be case insensitive.
    * Don't count occurrences of Java when searching for Javascript
    */
  def occurrencesOfLang(lang: String, rdd: RDD[WikipediaArticle]): Int =
    rdd
      .filter(
        article => article.mentionsLanguage(lang)
      )
      .count()
      .toInt


  /* (1) Use `occurrencesOfLang` to compute the ranking of the languages
   *     (`val langs`) by determining the number of Wikipedia articles that
   *     mention each language at least once. Don't forget to sort the
   *     languages by their occurrence, in decreasing order!
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangs(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    //use - to sort in reverse order
    langs
      .map(lang => (lang, occurrencesOfLang(lang, rdd)))
      .sortBy(tuple => -tuple._2)
  }


  /* Compute an inverted index of the set of articles, mapping each language
   * to the Wikipedia pages in which it occurs.
   */
  def makeIndex(langs: List[String], rdd: RDD[WikipediaArticle]): RDD[(String, Iterable[WikipediaArticle])] = {
    /*
      languagesUsedInArticle = [language1, language2, language3]
      We need to make a tuple, so we need to use map (to have a single value)
      for each language in article, create a tuple with that article like (language1, article1), (language2, article1)
      flatmap lets you have multiple languages outputted
      article -> language1, language2, language3
      groupbykey allows you to coalese the articles associated with a particular language
      want: (language, [article1, article2])
     */
    rdd.flatMap(article => {
      val words = article.text.split(" ").distinct
      words.map(word => (word,article))
    }).filter(x => langs.contains(x._1)).groupByKey

  }
  /* (2) Compute the language ranking again, but now using the inverted index. Can you notice
   *     a performance improvement?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsUsingIndex(index: RDD[(String, Iterable[WikipediaArticle])]): List[(String, Int)] = {
    /*
    have (language, [article1, article2, ...), (language, [article1, article2, ...), (language, [article1, article2, ...)
    want [(Java, 10), (Scala, 4), (Go, 1)]
    */

    index.mapValues(_.size).sortBy(-_._2).collect().toList
  }

  /* (3) Use `reduceByKey` so that the computation of the index and the ranking are combined.
   *     Can you notice an improvement in performance compared to measuring *both* the computation of the index
   *     and the computation of the ranking? If so, can you think of a reason?
   *
   *   Note: this operation is long-running. It can potentially run for
   *   several seconds.
   */
  def rankLangsReduceByKey(langs: List[String], rdd: RDD[WikipediaArticle]): List[(String, Int)] = {
    rdd.flatMap{article =>
      val words = article.text.split(" ").distinct
      words.map(word => (word,1))
    }.filter(x => langs.contains(x._1)).reduceByKey((a: Int,b: Int) => a+b).collect.toList
  }

  def main(args: Array[String]) {

    /* Languages ranked according to (1) */
    val langsRanked: List[(String, Int)] = timed("Part 1: naive ranking", rankLangs(langs, wikiRdd))

    /* An inverted index mapping languages to wikipedia pages on which they appear */
    def index: RDD[(String, Iterable[WikipediaArticle])] = makeIndex(langs, wikiRdd)

    /* Languages ranked according to (2), using the inverted index */
    val langsRanked2: List[(String, Int)] = timed("Part 2: ranking using inverted index", rankLangsUsingIndex(index))

    /* Languages ranked according to (3) */
    val langsRanked3: List[(String, Int)] = timed("Part 3: ranking using reduceByKey", rankLangsReduceByKey(langs, wikiRdd))

    /* Output the speed of each ranking */
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
