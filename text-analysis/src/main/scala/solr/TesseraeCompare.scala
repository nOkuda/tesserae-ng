/**
 * Author: Chris Eberle <eberle1080@gmail.com>
 *
 * Compare one set of solr documents to another
 */

package org.apache.solr.handler.tesserae

import org.apache.solr.handler._
import org.apache.solr.request.SolrQueryRequest
import org.apache.solr.response.SolrQueryResponse
import org.apache.solr.common.util.NamedList
import org.apache.solr.search._
import org.apache.solr.common.params.CommonParams
import org.apache.solr.common.SolrException
import org.apache.lucene.index.{DocsAndPositionsEnum, TermsEnum, IndexReader}
import org.apache.lucene.util.BytesRef
import org.slf4j.LoggerFactory
import collection.parallel.immutable.ParVector
import collection.parallel.ForkJoinTaskSupport
import scala.concurrent.forkjoin.ForkJoinPool
import net.sf.ehcache.{Element, Ehcache}
import org.apache.solr.handler.tesserae.metrics.CommonMetrics
import org.tesserae.EhcacheManager
import java.io.{FileWriter, File}
import org.apache.solr.analysis.corpus.LatinCorpusDatabase

import collection.mutable.{Map => MutableMap, Set => MutableSet, MutableList,
                           HashMap => MutableHashMap, HashSet => MutableHashSet}
import java.util.UUID
import java.io._

import org.tesserae.utils.TimerUtils.{time, timeQuietly}
import org.tesserae.utils.TimerUtils

final class TesseraeCompareHandler extends RequestHandlerBase {

  import DataTypes._
  import TesseraeCompareHandler._

  private lazy val logger = LoggerFactory.getLogger(getClass)
  private val DEFAULT_MAX_THREADS =
    Runtime.getRuntime.availableProcessors() + 1

  private var maximumThreads = DEFAULT_MAX_THREADS
  private var workerPool: ForkJoinPool = null
  private var corpusDB: LatinCorpusDatabase = null
  private var cache: Ehcache = null
  private var filterPositions = false

  override def init(args: NamedList[_]) {
    super.init(args)

    val corpusDbLoc = args.get("corpusFreqDBLocation") match {
      case null => throw new IllegalArgumentException("Can't initialize TesseraeCompareHandler, missing 'corpusFreqDBLocation' parameter")
      case str: String => str
    }

    args.get("filterPositions") match {
      case null => // do nothing
      case str: String => filterPositions = str.toBoolean
      case b: Boolean => filterPositions = b
    }

    maximumThreads = args.get("threads") match {
      case null => DEFAULT_MAX_THREADS
      case str: String => str.toInt
      case i: Int => i
    }

    if (workerPool != null) {
      workerPool.shutdown()
    }

    workerPool = new ForkJoinPool(maximumThreads)
    logger.info("Initialized worker pool with a max of " + plural(maximumThreads, "thread", "threads"))

    val ehcacheStr = args.get("cacheName").asInstanceOf[String]
    cache = EhcacheManager.compareCache(Option(ehcacheStr))

    val corpusEhcacheStr = Option(args.get("corpusCacheName").asInstanceOf[String])
    val corpusDir = new File(corpusDbLoc)

    corpusDB = new LatinCorpusDatabase(corpusEhcacheStr, corpusDir)

    logger.info("Initialized Ehcache")
  }

  def handleRequestBody(req: SolrQueryRequest, rsp: SolrQueryResponse) {
    implicit val context = RequestContext(UUID.randomUUID().toString)

    time("total request time", enabled=false) {
      CommonMetrics.compareOps.mark()
      val ctx = CommonMetrics.compareTime.time()
      try {
        internalHandleRequestBody(req, rsp)
      } catch {
        case e: Exception =>
          CommonMetrics.compareExceptions.mark()
          logger.error("Unhandled exception: " + e.getMessage, e)
          throw e
      } finally {
        ctx.stop()
      }
    }

    TimerUtils.printRequestStats
  }

  private def getTopNByFreq(n: Int, freqInfo: FrequencyMap)(implicit context: RequestContext): List[TermFrequencyEntry] = {
    var tmp = new MutableList[TermFrequencyEntry]
    val dummy = TermFrequencyEntry("", -1.0)
    for (_ <- 1 to n) {
      tmp += dummy
    }

    var lowestFreq = -1.0
    var lowestPos = 0
    for ((term, freq) <- freqInfo) {
      if (freq > tmp(lowestPos).frequency) {
        tmp(lowestPos) = TermFrequencyEntry(term, freq)
      }
      lowestFreq = tmp(0).frequency
      lowestPos = 0
      for (i <- 1 to n-1) {
        if (tmp(i).frequency < lowestFreq) {
          lowestFreq = tmp(i).frequency
          lowestPos = i
        }
      }
    }

    var frequencies : List[TermFrequencyEntry] = Nil
    tmp.foreach { entry =>
      if (entry.frequency > 0.0) {
        frequencies = entry :: frequencies
      }
    }
    frequencies
  }

  private def buildStopListByFreq(n: Int, freqInfo: FrequencyMap)(implicit context: RequestContext): MutableSet[String] = {
    var stoplist: MutableSet[String] = new MutableHashSet[String]
    val topNByFreq = getTopNByFreq(n, freqInfo)
    topNByFreq.foreach { entry =>
      stoplist += entry.term
    }
    stoplist
  }

  private def buildStopListByFreqs(n: Int, freqInfo1: FrequencyMap, freqInfo2: FrequencyMap)(implicit context: RequestContext): MutableSet[String] = {
    var freqMap = new MutableHashMap[String, Double]
    for ((term, freq) <- freqInfo1) {
      if (freqInfo2.contains(term)) {
        freqMap += term -> (freq + freqInfo2(term)) / 2.0
      }
    }
    buildStopListByFreq(n, freqMap)
  }

  private def internalHandleRequestBody(req: SolrQueryRequest, rsp: SolrQueryResponse)(implicit context: RequestContext) {
    val params = req.getParams
    val returnFields = new SolrReturnFields(req)
    rsp.setReturnFields(returnFields)

    rsp.add("params", req.getParams.toNamedList)

    var flags = 0
    if (returnFields.wantsScore) {
      flags |= SolrIndexSearcher.GET_SCORES
    }

    val start = params.getInt(CommonParams.START, 0)
    val rows = params.getInt(CommonParams.ROWS, 10)
    val maxDistance = params.getInt(TesseraeCompareParams.MD, DEFAULT_MAX_DISTANCE)
    val stopWords = params.getInt(TesseraeCompareParams.SW, DEFAULT_STOP_WORDS)
    val scoreCutoff = params.getDouble(TesseraeCompareParams.CUT, DEFAULT_SCORE_CUTOFF)
    val minCommonTerms = params.getInt(TesseraeCompareParams.MCT, DEFAULT_MIN_COMMON_TERMS)
    if (minCommonTerms < 2) {
      throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "min common terms can't be less than 2")
    }

    val callerStopListString = params.get(TesseraeCompareParams.SL, "").trim
    val stbasis = params.get(TesseraeCompareParams.SB, DEFAULT_STOP_BASIS)
    val freqbasis = params.get(TesseraeCompareParams.FB, DEFAULT_FREQ_BASIS)

    val metricStr = params.get(TesseraeCompareParams.METRIC)
    val metric = if (metricStr == null) {
      DistanceMetrics.DEFAULT_METRIC
    } else {
      DistanceMetrics(metricStr) match {
        case None => throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "invalid metric: " + metricStr)
        case Some(m) => m
      }
    }

    val sourceParams = QueryParameters(TesseraeCompareParams.SQ, TesseraeCompareParams.SF, TesseraeCompareParams.SFL)
    val targetParams = QueryParameters(TesseraeCompareParams.TQ, TesseraeCompareParams.TF, TesseraeCompareParams.TFL)

    val readCache = params.getBool(TesseraeCompareParams.RC, true)
    val writeCache = params.getBool(TesseraeCompareParams.WC, true)

    val cacheKey =
      CacheKey(maxDistance, minCommonTerms, metric,
        params.get(TesseraeCompareParams.SQ), params.get(TesseraeCompareParams.SF), params.get(TesseraeCompareParams.SFL),
        params.get(TesseraeCompareParams.TQ), params.get(TesseraeCompareParams.TF), params.get(TesseraeCompareParams.TFL),
        stopWords, callerStopListString, stbasis)

    var cachedResults: Option[CacheValue] = None
    if (readCache) {
      cache.get(cacheKey) match {
        case null => // not found
        case elem: Element => {
          // found maybe
          elem.getObjectValue match {
            case null =>
            // oh well
            case cv: CacheValue =>
              cachedResults = Some(cv)
            case _ =>
            // too bad
          }
        }
      }
    }

    val (sortedResults, sourceFieldList, targetFieldList, stoplist, fromCache) = cachedResults match {
      case None => {
        val ctx = CommonMetrics.uncachedCompareTime.time()
        val file = new File("/tmp/test.txt")
        val bw = new BufferedWriter(new FileWriter(file))
        try {
          bw.write("Starting calculations\n")
          val paramsVector = ParVector(sourceParams, targetParams)
          paramsVector.tasksupport = new ForkJoinTaskSupport(workerPool)
          val gatherInfoResults = paramsVector.map { qp: QueryParameters => gatherInfo(req, rsp, qp) }.toList
          val sourceInfo = gatherInfoResults(0)
          val targetInfo = gatherInfoResults(1)
          bw.write("Query for stbasis: " + stbasis + "\n")
          val _stoplist: MutableSet[String] = if (stopWords <= 0) {
            new MutableHashSet
          } else if (!callerStopListString.isEmpty) {
            val tmp = new MutableHashSet[String]
            SPLIT_REGEX.split(callerStopListString).toList.foreach { stopWord =>
              if (tmp.size < stopWords) {
                tmp += stopWord
              }
            }

            tmp
          } else if (stbasis == "both") {
            val sourceLemmaFreqs = getFrequencies(buildLemmaFrequencies(sourceInfo))
            val targetLemmaFreqs = getFrequencies(buildLemmaFrequencies(targetInfo))
            val rv = buildStopListByFreqs(stopWords, sourceLemmaFreqs, targetLemmaFreqs)
            bw.write(stopWords.toString + "\n")
            bw.write(rv.size + "\n")
            bw.write("Completed\n")
            rv
          } else if (stbasis == "source") {
            val sourceLemmaFreqs = getFrequencies(buildLemmaFrequencies(sourceInfo))
            val rv = buildStopListByFreq(stopWords, sourceLemmaFreqs)
            bw.write(stopWords.toString + "\n")
            bw.write(rv.size + "\n")
            bw.write("Completed\n")
            rv
          } else if (stbasis == "target") {
            val targetLemmaFreqs = getFrequencies(buildLemmaFrequencies(targetInfo))
            val rv = buildStopListByFreq(stopWords, targetLemmaFreqs)
            bw.write(stopWords.toString + "\n")
            bw.write(rv.size + "\n")
            bw.write("Completed\n")
            rv
          } else {
            // stbasis == "corpus"
            val rv = corpusDB.getTopN(stopWords)
            bw.write(stopWords.toString + "\n")
            bw.write(rv.size + "\n")
            bw.write("Completed\n")
            rv
          }

          (compare(sourceInfo, targetInfo, freqbasis, maxDistance, _stoplist, scoreCutoff, minCommonTerms, metric),
            sourceInfo.fieldList, targetInfo.fieldList, _stoplist, false)
        } finally {
          ctx.stop()
          bw.close()
        }
      }
      case Some(cv) => {
        (cv.results, cv.sourceFieldList, cv.targetFieldList, cv.stoplist, true)
      }
    }

    if (writeCache && !fromCache) {
      val value = CacheValue(sortedResults, sourceFieldList, targetFieldList, stoplist)
      val elem = new Element(cacheKey, value)
      cache.put(elem)
    }

    time("formatting", enabled=false) {
      val timer = CommonMetrics.resultsFormattingTime.time()
      val file = new File("/tmp/post.txt")
      val bw = new BufferedWriter(new FileWriter(file))
      try {
        bw.write("Started\n")
        val results = sortedResults.drop(start).take(rows)
        val totalResultCount = sortedResults.length

        val searcher = req.getSearcher
        val reader = searcher.getIndexReader

        def processResult(result: CompareResult, sourceIfTrue: Boolean, populate: DocFields) = {
          val (docId, fieldList) =
            if (sourceIfTrue) (result.pair.sourceDoc, sourceFieldList)
            else (result.pair.targetDoc, targetFieldList)

          val doc = reader.document(docId)
          var found = 0
          fieldList.foreach { fieldName =>
            val fieldValue = doc.get(fieldName)
            if (fieldValue != null) {
              found += 1
              populate.put(fieldName, fieldValue)
            }
          }

          found
        }

        val stoplistList = new StopList
        stoplist.foreach { term =>
          stoplistList.add(term)
        }

        val matches = new TesseraeMatches
        var rank = start
        results.foreach { result =>
          rank += 1
          val m = new TesseraeMatch
          m.put("rank", new java.lang.Integer(rank))
          m.put("score", new java.lang.Double(result.score))
          m.put("distance", new java.lang.Double(result.distance))

          val terms = new TermList
          result.commonTerms.toList.sorted.foreach { term =>
            terms.add(term)
          }

          m.put("terms", terms)

          val sdoc = new TesseraeDoc
          val tdoc = new TesseraeDoc

          val sourceFields = new DocFields
          if (processResult(result, sourceIfTrue=true, sourceFields) > 0) {
            sdoc.put("fields", sourceFields)
          }

          val targetFields = new DocFields
          if (processResult(result, sourceIfTrue=false, targetFields) > 0) {
            tdoc.put("fields", targetFields)
          }

          m.put("source", sdoc)
          m.put("target", tdoc)

          matches.add(m)
        }

        rsp.add("stopList", stoplistList)

        rsp.add("matchTotal", totalResultCount)
        rsp.add("matchCount", results.length)
        rsp.add("matchOffset", start)

        rsp.add("matches", matches)
        rsp.add("cached", fromCache)
      } finally {
        timer.stop()
        bw.write("Completed\n")
        bw.close()
      }
    }
  }

  private def getMetric(distanceMetric: DistanceMetrics.Value, maxDistance: Int): Distance = {
    val dm = DistanceMetrics(distanceMetric)
    dm match {
      case None => throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "invalid metric: " + distanceMetric.toString)
      case Some(d) => d
    }
  }

  private def deduplicate(docPairs: MutableMap[DocumentPair, DocumentPairInfo])(implicit context: RequestContext): Map[DocumentPair, DocumentPairInfo] = {
    time("deduplicate", enabled=false) {
      val deduped = new MutableHashMap[DocumentPair, (DocumentPair, DocumentPairInfo)]
      docPairs.foreach { case (pair, info) =>
        val first = pair.sourceDoc
        val second = pair.targetDoc
        val (a, b) = if (first > second) {
          (second, first)
        } else {
          (first, second)
        }

        val key = DocumentPair(a, b)
        if (!deduped.contains(key)) {
          deduped += key -> (pair, info)
        }
      }

      deduped.map { case (_, (pair, set)) => pair -> set }.toMap
    }
  }

  private def getFrequencies(freqInfo: AggregateTermInfo)(implicit context: RequestContext): FrequencyMap = {
    time("getFrequencies", enabled=false) {
      var freqMap: FrequencyMap = new MutableHashMap
      val total = freqInfo.totalTermCount.toDouble
      freqInfo.termCounts.foreach { case (term, count) =>
        val frequency = count.toDouble / total
        freqMap += term -> frequency
      }

      freqMap
    }
  }

  private def dumpFrequencyInfo(timestamp: Long, freq: AggregateTermInfo, sourceIfTrue: Boolean) {
    val root_dir = new File("/opt/data/freq")
    if (!root_dir.exists()) {
      root_dir.mkdirs()
    }

    val ts_dir = new File(root_dir, timestamp.toString)
    if (!ts_dir.exists()) {
      ts_dir.mkdirs()
    }

    val filename = new File(ts_dir, if (sourceIfTrue) "source_freq.txt" else "target_freq.txt")

    val fw = new FileWriter(filename)
    try {
      fw.write("# count: " + freq.totalTermCount + "\n")

      var frequencies: List[(String, Int)] = Nil
      freq.termCounts.foreach { case tuple =>
        frequencies = tuple :: frequencies
      }

      val sorted = frequencies.sortWith((a, b) => a._2 > b._2)
      sorted.foreach { case (term, count) =>
        fw.write(term)
        fw.write("\t")
        fw.write(count.toString)
        fw.write("\n")
      }
    } finally {
      fw.close()
    }
  }

  private def buildMashAndFreq(source: QueryInfo, target: QueryInfo)(implicit context: RequestContext): (Mash, FrequencyMap, Mash, FrequencyMap) = {
    // A mash maps one term to a set of document ids. Build them in parallel.
    val parvector = ParVector((source, true), (target, false))
    parvector.tasksupport = new ForkJoinTaskSupport(workerPool)

    val mashAndFreqResults = time("buildMash & frequency stuff", enabled=false) {
      parvector.map { case (qi: QueryInfo, b: Boolean) =>
        val mash = buildMash(qi)
        val frequencyInfo = buildTermFrequencies(qi)
        val digested = getFrequencies(frequencyInfo)
        (mash, digested)
      }.toList
    }

    val (sourceMash, sourceFrequencies) = mashAndFreqResults(0)
    val (targetMash, targetFrequencies) = mashAndFreqResults(1)

    (sourceMash, sourceFrequencies, targetMash, targetFrequencies)
  }

  private def buildTextCorpusFrequencies(queryInfo: QueryInfo, corpusNonFormFrequencies: FrequencyMap)(implicit context: RequestContext): FrequencyMap = {
    time("buildTextCorpusFrequencies", enabled=false) {
      val countByWord: MutableMap[String, Int] = new MutableHashMap
      val byFeature: MutableMap[String, MutableSet[String]] = new MutableHashMap
      val wordsToStems: MutableMap[String, MutableSet[String]] = new MutableHashMap

      queryInfo.termInfo.foreach { case (docId, dti) =>
        dti.formTermCounts.foreach { case (term, count) =>
          val theCount = count + countByWord.getOrElse(term, 0)
          countByWord += term -> theCount
        }

        dti.nonFormTermCounts.foreach { case (term, count) =>
          val form = dti.nf2f(term).term
          val set1: MutableSet[String] = byFeature.get(term) match {
            case Some(s) => s
            case None => {
              val tmp = new MutableHashSet[String]
              byFeature += term -> tmp
              tmp
            }
          }

          set1 += form

          val set2: MutableSet[String] = wordsToStems.get(form) match {
            case Some(s) => s
            case None => {
              val tmp = new MutableHashSet[String]
              wordsToStems += form -> tmp
              tmp
            }
          }

          set2 += term
        }
      }

      val finalFrequencies = new MutableHashMap[String, Double]

      val file = new File("/tmp/textCorpFreqs.txt")
      val bw = new BufferedWriter(new FileWriter(file))
      try {
      for ((term, lemmata) <- wordsToStems) {
        var freq_values: Double = 0.0
        lemmata.foreach { lemma =>
          freq_values += corpusNonFormFrequencies.getOrElse(lemma, 0.0)
        }
        bw.write(term + "\t" + freq_values.toString + lemmata.size.toString + "\n")
        finalFrequencies += term -> freq_values / lemmata.size
      }
      } finally {
      bw.close()
      }

      finalFrequencies
    }
  }

  private def compare(source: QueryInfo,
                      target: QueryInfo,
                      freqBasis: String,
                      maxDistance: Int, stoplist: MutableSet[String], scoreCutoff: Double,
                      minCommonTerms: Int, distanceMetric: DistanceMetrics.Value)(implicit context: RequestContext): List[CompareResult] = {

    time("compare", enabled=false) {
      val (sourceMash, sourceFrequencies, targetMash, targetFrequencies) = buildMashAndFreq(source, target)

      // Find the overlapping documents
      val foundPairs = time ("findDocumentPairs", enabled=false) {
        val file = new File("/tmp/stop.txt")
        val bw = new BufferedWriter(new FileWriter(file))
        val fpTimer = CommonMetrics.findDocumentPairs.time()
        try {
          stoplist.foreach { stopword =>
            bw.write(stopword + "\n")
          }
          findDocumentPairs(sourceMash, targetMash, stoplist)
        } finally {
          fpTimer.stop()
          bw.close()
        }
      }

      val file = new File("/tmp/compare.txt")
      val bw = new BufferedWriter(new FileWriter(file))
      try {
      // Only consider documents with 2 or more unique terms in common
      bw.write("Starting filteredPairs\n")
      val filteredPairs = time("filter pairs", enabled=false) { foundPairs.filter { case (_, dpi) => {
        dpi.targetTerms.size >= minCommonTerms && dpi.sourceTerms.size >= minCommonTerms
      }}}

      bw.write("Starting deduplicate\n")
      // If there's an (a, b) and a (b, a) match, remove one
      val docPairs = deduplicate(filteredPairs)

      bw.write("Starting getMetric\n")
      val distMetric = getMetric(distanceMetric, maxDistance)

      bw.write("Starting DigestedFrequencyInfo\n")
      val frequencyInfo = if (freqBasis != "corpus") {
        // Build up information about the source and target frequencies
        DigestedFrequencyInfo(sourceFrequencies, targetFrequencies)
      } else {
        // Build up information about corpus frequencies
        val corpusNonFormFrequencies = corpusDB.getCorpusFrequencies()
        bw.write("Finished corpus frequencies\n")
        val sourceCorpusFrequencies = buildTextCorpusFrequencies(source, corpusNonFormFrequencies)
        bw.write("Finished source corpus frequencies\n")
        val targetCorpusFrequencies = buildTextCorpusFrequencies(target, corpusNonFormFrequencies)
        bw.write("Finished target corpus frequencies\n")
        DigestedFrequencyInfo(sourceCorpusFrequencies, targetCorpusFrequencies)
      }

      // Calculate the scores and distances in parallel
      val parallelPairs = docPairs.par
      parallelPairs.tasksupport = new ForkJoinTaskSupport(workerPool)

      bw.write("Starting parallel\n")
      val mappedResults = time("calculate distance & score", enabled=false) {
        parallelPairs.map { case (pair: DocumentPair, pairInfo: DocumentPairInfo) =>
          val sourceTerms = pairInfo.sourceTerms.keySet.map { st => st.term }.toSet
          val targetTerms = pairInfo.targetTerms.keySet.map { tt => tt.term }.toSet
          val distanceParams = DistanceParameters(pair, source, target, frequencyInfo, sourceTerms, targetTerms)

          distMetric.calculateDistance(distanceParams) match {
            case None => None
            case Some(distance) => {
              if (distance <= maxDistance || maxDistance <= 0) {
                var score = 0.0
                var skip = false

                val seenNonForms: MutableSet[String] = new MutableHashSet

                pairInfo.sourceTerms.foreach { case (TermPosition(term, _, _, _), nonForms) =>
                  seenNonForms += nonForms.toList.sorted.mkString("-")
                  val sourceScore = 1.0 / frequencyInfo.sourceFrequencies.getOrElse(term, -1.0)
                  if (sourceScore < 0.0) {
                    logger.warn("No source term frequency information available for term: `" + term + "'")
                    skip = true
                  } else {
                    score += sourceScore
                  }
                }

                pairInfo.targetTerms.foreach { case (TermPosition(term, _, _, _), nonForms) =>
                  seenNonForms += nonForms.toList.sorted.mkString("-")
                  val targetScore = 1.0 / frequencyInfo.targetFrequencies.getOrElse(term, -1.0)
                  if (targetScore < 0.0) {
                    logger.warn("No target term frequency information available for term: `" + term + "'")
                    skip = true
                  } else {
                    score += targetScore
                  }
                }

                val finalScore = math.log(score / distance.toDouble)
                if (skip || finalScore < scoreCutoff || finalScore.isNaN) {
                  None
                } else {
                  val result = CompareResult(pair, seenNonForms, finalScore, distance)
                  Some(result)
                }
              } else {
                None
              }
            }
          }
        }.filter(_.isDefined).map(_.get)
      }

      bw.write("Starting sort\n")
      // Sort by score
      val sortedScores = mappedResults.toList.sortWith { (a, b) => a.score > b.score }
      bw.write("Completed\n")
      sortedScores
      } finally {
        bw.close()
      }
    }
  }

  private def buildLemmaFrequencies(queryInfo: QueryInfo)(implicit context: RequestContext): AggregateTermInfo = {
    time("buildLemmaFrequencies", enabled=false) {
      val countByWord: MutableMap[String, Int] = new MutableHashMap
      var totalWords = 0

      queryInfo.termInfo.foreach { case (docId, dti) =>
        dti.nonFormTermCounts.foreach { case (term, count) =>
          val theCount = count + countByWord.getOrElse(term, 0)
          countByWord += term -> theCount
          totalWords += count
        }
      }

      AggregateTermInfo(countByWord, totalWords)
    }
  }

  private def buildTermFrequencies(queryInfo: QueryInfo)(implicit context: RequestContext): AggregateTermInfo = {
    time("buildTermFrequencies", enabled=false) {
      val countByWord: MutableMap[String, Int] = new MutableHashMap
      val byFeature: MutableMap[String, MutableSet[String]] = new MutableHashMap
      val wordsToStems: MutableMap[String, MutableSet[String]] = new MutableHashMap

      var totalWords = 0

      queryInfo.termInfo.foreach { case (docId, dti) =>
        dti.formTermCounts.foreach { case (term, count) =>
          val theCount = count + countByWord.getOrElse(term, 0)
          countByWord += term -> theCount
          totalWords += count
        }

        dti.nonFormTermCounts.foreach { case (term, count) =>
          val form = dti.nf2f(term).term
          val set1: MutableSet[String] = byFeature.get(term) match {
            case Some(s) => s
            case None => {
              val tmp = new MutableHashSet[String]
              byFeature += term -> tmp
              tmp
            }
          }

          set1 += form

          val set2: MutableSet[String] = wordsToStems.get(form) match {
            case Some(s) => s
            case None => {
              val tmp = new MutableHashSet[String]
              wordsToStems += form -> tmp
              tmp
            }
          }

          set2 += term
        }
      }

      val countByFeature: MutableMap[String, Int] = new MutableHashMap

      countByWord.keySet.foreach { word1 =>
        val alreadySeen: MutableSet[String] = new MutableHashSet
        wordsToStems.get(word1).map { w1 =>
          w1.foreach { case key =>
            byFeature.get(key).map { bf =>
              bf.foreach { word2 =>
                if (!alreadySeen.contains(word2)) {
                  val priorCount = countByFeature.getOrElse(word1, 0)
                  countByFeature += word1 -> (priorCount + countByWord.getOrElse(word2, 0))
                  alreadySeen += word2
                }
              }
            }
          }
        }
      }

      AggregateTermInfo(countByFeature, totalWords)
    }
  }

  private def findDocumentPairs(sourceMash: Mash, targetMash: Mash, stoplist: MutableSet[String])(implicit context: RequestContext): MutableMap[DocumentPair, DocumentPairInfo] = {

    val match_target: TargetToSourceToF2NF = new MutableHashMap
    val match_source: TargetToSourceToF2NF = new MutableHashMap

    val file = new File("/tmp/find.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    try {
    bw.write("first loop\n")
    // populate match_target and match_source
    time("first loop (findDocumentPairs)", enabled=false) {
    bw.write("in time 1\n")
      sourceMash.nonFormsToDocs.foreach { case (term, sourceDocs) =>
        if (!targetMash.nonFormsToDocs.contains(term)) {
          // continue
        } else if (stoplist.contains(term)) {
          // continue
          bw.write("in foreach 1 (found stoplist term \""+ term + "\")\n")
        } else {
          val targetDocs = targetMash.nonFormsToDocs(term)
          sourceDocs.foreach { sourceDocId =>
            val sourceInfo = sourceMash.docInfo(sourceDocId)
            val sourceForm = sourceInfo.nf2f(term)

            targetDocs.foreach { targetDocId =>
              val targetInfo = targetMash.docInfo(targetDocId)
              val targetForm = targetInfo.nf2f(term)

              val mtSet = match_target.getOrElseUpdate(targetDocId, new MutableHashMap).
                getOrElseUpdate(sourceDocId, new MutableHashMap).
                getOrElseUpdate(targetForm, new MutableHashSet)

              val msSet = match_source.getOrElseUpdate(targetDocId, new MutableHashMap).
                getOrElseUpdate(sourceDocId, new MutableHashMap).
                getOrElseUpdate(sourceForm, new MutableHashSet)

              mtSet += term
              msSet += term
            }
          }
        }
      }
    }

    val pairInfo: MutableMap[DocumentPair, DocumentPairInfo] = new MutableHashMap

    bw.write("second loop\n")
    time("second loop (findDocumentPairs)", enabled=false) {
    bw.write("in time 2\n")
      match_target.foreach { case (targetDocId, sourceToF2NF) =>
        sourceToF2NF.foreach { case (sourceDocId, f2nf) =>
          if (sourceDocId == targetDocId) {
            // continue
          } else {
            val l1: SourceToF2NF = match_source.getOrElse(targetDocId, new MutableHashMap)
            if (f2nf.size < 2) {
              sourceToF2NF.remove(sourceDocId)
              l1.remove(sourceDocId)
              // continue
            } else {
              val l2: FormToNonForms = l1.getOrElse(sourceDocId, new MutableHashMap)
              if (l2.size < 2) {
                sourceToF2NF.remove(sourceDocId)
                l1.remove(sourceDocId)
                // continue
              } else {
                val seenForms: MutableSet[String] = new MutableHashSet
                f2nf.keySet.foreach { form =>
                  seenForms += form.term
                }

                if (seenForms.size < 2) {
                  // continue
                } else {
                  seenForms.clear()
                  l2.keySet.foreach { form =>
                    seenForms += form.term
                  }

                  if (seenForms.size < 2) {
                    // continue
                  } else {
                    bw.write("in foreach 2\n")
                    val pair = DocumentPair(sourceDocId, targetDocId)
                    val dpi = pairInfo.getOrElseUpdate(pair, DocumentPairInfo(new MutableHashMap, new MutableHashMap))
                    dpi.targetTerms ++= f2nf
                    dpi.sourceTerms ++= l2
                  }
                }
              }
            }
          }
        }
      }
    }
    bw.write("Completed\n")

    pairInfo
    } finally {
      bw.close()
    }
  }

  private def buildMash(qi: QueryInfo)(implicit context: RequestContext): Mash = {
    time("buildMash", enabled=false) {
      val formMap: TermDocumentMap = new MutableHashMap
      val nonFormMap: TermDocumentMap = new MutableHashMap

      qi.termInfo.foreach { case (docId, termInfo) =>
        termInfo.formTermCounts.keySet.foreach { term =>
          val docSet = formMap.getOrElseUpdate(term, new MutableHashSet[Int])
          docSet += docId
        }
        termInfo.nonFormTermCounts.keySet.foreach { term =>
          val docSet = nonFormMap.getOrElseUpdate(term, new MutableHashSet[Int])
          docSet += docId
        }
      }

      Mash(formMap, nonFormMap, qi.termInfo)
    }
  }

  private def gatherInfo(req: SolrQueryRequest, rsp: SolrQueryResponse, qParams: QueryParameters)(implicit context: RequestContext): QueryInfo = {
    time("gatherInfo", enabled=false) {
      val params = req.getParams
      val defType = params.get(QueryParsing.DEFTYPE, QParserPlugin.DEFAULT_QTYPE)

      val queryStr = params.get(qParams.qParamName)
      if (queryStr == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "missing parameter: " + qParams.qParamName)
      }

      val parser = QParser.getParser(queryStr, defType, req)
      val query = parser.getQuery
      //val sorter = parser.getSort(true)
      val searcher = req.getSearcher
      val reader = searcher.getIndexReader

      logger.info("My reader is: " + String.valueOf(reader.getClass))

      val searchField = params.get(qParams.searchFieldParamName)
      if (searchField == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "missing parameter: " + qParams.searchFieldParamName)
      }

      val fieldList = params.get(qParams.fieldListParamName)
      if (fieldList == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "missing parameter: " + qParams.fieldListParamName)
      }

      val schema = req.getSchema
      val sf = schema.getFieldOrNull(searchField)
      if (sf == null) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "invalid search field: " + searchField)
      }
      if (!sf.storeTermVector()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "field " + searchField + " doesn't store term vectors")
      }
      if (!sf.storeTermPositions()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "field " + searchField + " doesn't store term positions")
      }
      if (!sf.storeTermOffsets()) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "field " + searchField + " doesn't store term offsets")
      }

      val returnFields = SPLIT_REGEX.split(fieldList).toList
      if (returnFields.isEmpty) {
        throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "field list can't be empty: " + qParams.fieldListParamName)
      }

      returnFields.foreach { fieldName =>
        val field = schema.getFieldOrNull(fieldName)
        if (field == null) {
          throw new SolrException(SolrException.ErrorCode.BAD_REQUEST, "invalid field in field list: " + fieldName)
        }
      }

      val secondParam: java.util.List[org.apache.lucene.search.Query] = null
      val listAndSet = time("execute search", enabled=false) { searcher.getDocListAndSet(query, secondParam, null, 0, 100000) }
      val dlit = listAndSet.docSet.iterator()
      val documents: MutableSet[Int] = new MutableHashSet

      while (dlit.hasNext) {
        val docId = dlit.nextDoc()
        documents += docId
      }

      val parvec = ParVector(documents.toList.sorted :_*)
      parvec.tasksupport = new ForkJoinTaskSupport(workerPool)

      var termInfo: QueryTermInfo = new MutableHashMap
      val mappedResults = timeQuietly("get term vectors", enabled=false) { parvec.map { case docId =>

        val vec = reader.getTermVector(docId, searchField)
        (docId, mapOneVector(reader, docId, vec.iterator(null), searchField))
      }}

      mappedResults.toList.foreach { case (docId: Int, dti: DocumentTermInfo) =>
        termInfo += docId -> dti
      }

      logger.info("Using query `" + queryStr + "' found " + plural(termInfo.size, "result", "results"))
      QueryInfo(termInfo, returnFields, (offset, count) => listAndSet.docList)
    }
  }

  private def plural(i: Int, singular: String, plural: String): String = {
    i match {
      case 1 => "1 " + singular
      case n => n + " " + plural
    }
  }

  private def mapOneVector(reader: IndexReader, docId: Int, termsEnum: TermsEnum, field: String)(implicit context: RequestContext): DocumentTermInfo = {

    var rawText: BytesRef = termsEnum.next()
    var dpEnum: DocsAndPositionsEnum = null

    val formCounts: TermCountMap = new MutableHashMap
    val nonFormCounts: TermCountMap = new MutableHashMap
    val termPos: TermPositionsMap = new MutableHashMap
    val posInfo: MutableMap[(Int, Int), TermPositionInfo] = new MutableHashMap

    while (rawText != null) {
      val termText = rawText.utf8ToString
      val freq = termsEnum.totalTermFreq.toInt
      val (term, isForm) = if (termText.startsWith("_")) {
        (termText.substring(1), true)
      } else {
        (termText, false)
      }

      dpEnum = termsEnum.docsAndPositions(null, dpEnum)
      if (dpEnum == null) {
        throw new SolrException(SolrException.ErrorCode.SERVER_ERROR, "termsEnum.docsAndPositions returned null")
      }

      dpEnum.nextDoc()
      for (i <- 0 until freq) {
        val pos = dpEnum.nextPosition
        val start = dpEnum.startOffset()
        val end = dpEnum.endOffset()
        val posTuple = (start, end)
        val entry = TermPosition(term, docId, pos, posTuple)

        val info = posInfo.getOrElseUpdate(posTuple, TermPositionInfo(None, new MutableHashSet[String]()))
        if (isForm) {
          info.form = Some(entry)
        } else {
          info.nonForms += term
        }

        val termPosSet: MutableSet[TermPosition] = termPos.getOrElseUpdate(term, new MutableHashSet[TermPosition])

        termPosSet += entry

        if (isForm) {
          val oldCount = formCounts.getOrElse(term, 0)
          formCounts += term -> (oldCount + 1)
        } else {
          val oldCount = nonFormCounts.getOrElse(term, 0)
          nonFormCounts += term -> (oldCount + 1)
        }
      }

      rawText = termsEnum.next()
    }

    val nf2f: NonFormToFormMap = new MutableHashMap
    posInfo.foreach { case (position, info) =>
      info.form match {
        case Some(termForm) =>
          info.nonForms.foreach { term =>
            nf2f += term -> termForm
          }
        case None =>
          logger.warn("Missing a form: " + info)
      }
    }

    DocumentTermInfo(docId, formCounts, nonFormCounts, termPos, posInfo, nf2f)
  }

  def getDescription =
    "Tesserae two-document comparison"

  def getSource =
    "$URL: https://raw.github.com/eberle1080/tesserae-ng/master/text-analysis/src/main/scala/solr/TesseraeCompare.scala $"
}

object TesseraeCompareHandler {
  val DEFAULT_MAX_DISTANCE = 0 // 0 = no max
  val DEFAULT_STOP_WORDS = 10
  val DEFAULT_SCORE_CUTOFF = 0.0
  val DEFAULT_MIN_COMMON_TERMS = 2 // can't be less than 2
  val DEFAULT_STOP_BASIS = "corpus"
  val DEFAULT_FREQ_BASIS = "texts"
  val DEFAULT_HIGHLIGHT = false
  val SPLIT_REGEX = ",| ".r
}
