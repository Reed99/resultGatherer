import org.apache.spark.sql.functions._
import org.apache.spark.sql.{ Column, DataFrame, SparkSession }

import java.io.File
import scala.language.postfixOps

object Main {

  val groupNumber: Int = 1
  val okFileExtensions: Seq[String] = Seq("csv")
  val acceptedColName: String = "Is Accepted"
  val neptun: String = "NEPTUN"
  val name: String = "Name"
  val homeworkIndicatorInFileName: String = "Házi"
  val examIndicatorInFileName: String = "ZH"
  val pVzIndicatorInFileName: String = "Plants"
  val examNameCount: Int = 3
  val notes: String = "Notes"
  val pathToFiles: String = "/Users/bnadas/TMSFiles"
  val acceptedStatus: Seq[String] = Seq("Accepted", "Passed")
  val homeworkThreshold: Int = 11
  val theotyThreshold: Int = 3
  val nameOfCorrective: String = "zh5"
  val gradesMap: Map[Double, Int] = Map(21.0 -> 5, 18.0 -> 4, 15.0 -> 3, 12.0 -> 2)
  val getGrade = udf ( (score: Double) => gradesMap.find(_._1 <= score).map(_._2).getOrElse(1) )



  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .master("local").appName("Spark")
      .getOrCreate()

    spark.sparkContext.setLogLevel("error")

    val files: Seq[File] = getListOfFiles(new File(pathToFiles), okFileExtensions)

    val neptunCodes: DataFrame = readDF(files.head).select(neptun)

    val passedHomework: DataFrame = calculateHomework(files, neptunCodes)

    val examResults: DataFrame = calculateExam(files, neptunCodes)

    val pvzDF: DataFrame = calculatePVZ(files)

    val merged: DataFrame = examResults
      .join(passedHomework, Seq(neptun), "inner")
      .join(pvzDF, Seq(neptun), "inner")
      .select(lit(groupNumber).as("group_number"), col(name), col(neptun),
        (col("exam_points") + col("PVZ")).as("points"))
      .withColumn("grade", getGrade(col("points")))

    writeCSV(merged, "results")

  }

  def calculatePVZ(files: Seq[File])(implicit spark: SparkSession): DataFrame = {
    val pvzFile: File = files.filter(_.getName.contains(pVzIndicatorInFileName)).head
    val pvzColName = "PVZ"
    readDF(pvzFile)
      .filter(col(acceptedColName).isin(acceptedStatus: _*))
      .withColumn(pvzColName, col("Grade5"))
      .select(name, neptun, pvzColName)
}

  def calculateExam(files: Seq[File], neptunCodes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val exams: Seq[File] = files.filter(_.getName.contains(examIndicatorInFileName))
    val examsDFnonC: DataFrame = aggregateDF(exams, neptunCodes, notes, neptun)

    val possibleExams: List[String] = List.range(1, 10).map("zh" ++ _.toString)

    val examMap: Seq[(String, List[Column])] = possibleExams
      .map(y => y -> examsDFnonC
        .columns
        .filter(_.startsWith(y))
        .map(col).toList)
      .filterNot(_._2.isEmpty)

    val nonCorrectiveExams = examMap.map(_._1).filterNot(_.startsWith(nameOfCorrective))

    val examsDFCol: DataFrame = examMap.foldLeft(examsDFnonC) { case (acc, (na, li)) =>
      acc.withColumn(na, coalesce(li: _*))
    }

    val examsDF: DataFrame = examsDFCol.select(neptun, examMap.map(_._1): _*)

    val theoryPracDF: DataFrame = examMap.foldLeft(examsDF) { case (acc, (na, li)) =>
      val splitted: Column = split(col(na), "\\+")
      acc
        .withColumn(na + "_theory", splitted.getItem(0).cast("Double")).na.fill(0.0)
        .withColumn(na + "_prac", splitted.getItem(1).cast("Double")).na.fill(0.0)
    }.withColumn("summa",
      when(col(nameOfCorrective).isNull && theorySum(nonCorrectiveExams) >= theotyThreshold,
        nonCorrectiveExams
          .flatMap(t => withTheoryAndPractice(t).map(col))
          .reduce(_ + _))
        .otherwise(lit(0)))

    val toppedDf: DataFrame = removeEveryWay(examMap.map(_._1).toList).init.foldLeft(theoryPracDF) { (acc: DataFrame, posArrangement: List[String]) =>
      acc
        .withColumn(posArrangement.mkString(""),
          when(col(nameOfCorrective).isNotNull && theorySum(posArrangement) >= theotyThreshold,
            posArrangement
              .flatMap(t => withTheoryAndPractice(t).map(col))
              .reduce(_ + _))
            .otherwise(lit(0)))
    }

    writeCSV(toppedDf, "aggregatedExams")

    val strippedDF: DataFrame = toppedDf.select(toppedDf.columns.filterNot(examMap.map(_._1).contains(_)).map(col): _*)

    val maxpointsDF = strippedDF.select(col(neptun), expr(generateStackCall(strippedDF.columns, "result")))
      .groupBy(neptun)
      .agg(max("result").as("exam_points"))

    writeCSV(maxpointsDF, "examPoints")

    maxpointsDF

  }

  def calculateHomework(files: Seq[File], neptunCodes: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val homeworks: Seq[File] = files.filter(_.getName.contains(homeworkIndicatorInFileName))
    val homeworkDF: DataFrame = aggregateDF(homeworks, neptunCodes, acceptedColName, neptun)

    val countedHomeworkDF = homeworkDF.select(col(neptun), expr(generateStackCall(homeworkDF.columns, "homework")))
      .groupBy(neptun)
      .agg(sum(
        when(
          col("homework").isin(acceptedStatus: _*), 1)
          .otherwise(0)).as("accepted_homeworks"))
    writeCSV(countedHomeworkDF, "aggregatedHomeworks")

    val filteredHomework = countedHomeworkDF.filter(col("accepted_homeworks") >= homeworkThreshold)
    writeCSV(filteredHomework, "filteredHomework")
    filteredHomework
  }

  def withTheoryAndPractice(t: String): Seq[String] = Seq(t + "_theory", t + "_prac")

  def theorySum(cols: Seq[String]): Column = cols.map(t => col(withTheoryAndPractice(t).head)).reduce(_ + _)


  def writeCSV(
    df: DataFrame,
    name: String
  ): Unit = df.coalesce(1).write.option("header", true).mode("overwrite").csv(pathToFiles + '/' + name)

  def generateStackCall(cols: Array[String], name: String): String = {
    val fields: List[String] = cols.filterNot(_.startsWith(neptun)).toList
    s"stack(${fields.size}, ${fields.flatMap(t => Seq('\'' + t + '\'', t)).mkString(", ")} ) as (group, $name)"
  }

  def getListOfFiles(dir: File, extensions: Seq[String]): List[File] = {
    dir.listFiles.filter { file =>
      file.isFile && extensions.exists(file.getName.endsWith(_))
    }.toList
  }

  def sanitize(colname: String): Column = regexp_replace(regexp_replace(trim(col(colname)), "\n|\r", ""), "\\,", ".")

  def aggregateDF(files: Seq[File], inital: DataFrame, colname: String, joinOn: String)
    (implicit spark: SparkSession): DataFrame = {
    files.foldLeft(inital) { (acc: DataFrame, f: File) =>
      val name: String = f.getName.dropRight(4).toLowerCase().replaceAll("[^\\x00-\\x7F]", "").replace(" ", "")
      val currDf: DataFrame = readDF(f).withColumn(name, sanitize(colname)).select(joinOn, name)
      acc.join(currDf, Seq(joinOn), "fullouter")
    }
  }

  def readDF(f: File)
    (implicit spark: SparkSession): DataFrame = spark.read.option("header", value = true).option("multiLine", "true").csv(f.toString)

  def removeEveryWay[A](list: List[A]): List[List[A]] = {
    def removeEvery[A](list: List[A], index: Int): List[List[A]] = {
      if (index == list.length) Nil
      else (list.take(index) ++ list.drop(index + 1)) :: removeEvery(list, index + 1)
    }

    removeEvery(list, 0)
  }

}
