/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.hive.execution

import org.apache.gluten.config.GlutenConfig
import org.apache.gluten.execution.FileSourceScanExecTransformer

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.hive.{HiveExternalCatalog, HiveTableScanExecTransformer}
import org.apache.spark.sql.hive.client.HiveClient
import org.apache.spark.sql.internal.SQLConf

class GlutenHiveSQLQuerySuite extends GlutenHiveSQLQuerySuiteBase {

  override def sparkConf: SparkConf = {
    defaultSparkConf
      .set("spark.plugins", "org.apache.gluten.GlutenPlugin")
      .set("spark.default.parallelism", "1")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "1024MB")
  }

  testGluten("hive orc scan") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      sql("DROP TABLE IF EXISTS test_orc")
      sql(
        "CREATE TABLE test_orc (name STRING, favorite_color STRING)" +
          " USING hive OPTIONS(fileFormat 'orc')")
      sql("INSERT INTO test_orc VALUES('test_1', 'red')")
      val df = spark.sql("select * from test_orc")
      checkAnswer(df, Seq(Row("test_1", "red")))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("Add orc char type validation") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      sql("DROP TABLE IF EXISTS test_orc")
      sql(
        "CREATE TABLE test_orc (name char(10), id int)" +
          " USING hive OPTIONS(fileFormat 'orc')")
      sql("INSERT INTO test_orc VALUES('test', 1)")
    }

    def testExecPlan(
        convertMetastoreOrc: String,
        charTypeFallbackEnabled: String,
        shouldFindTransformer: Boolean,
        transformerClass: Class[_ <: SparkPlan]
    ): Unit = {

      withSQLConf(
        "spark.sql.hive.convertMetastoreOrc" -> convertMetastoreOrc,
        GlutenConfig.VELOX_FORCE_ORC_CHAR_TYPE_SCAN_FALLBACK.key -> charTypeFallbackEnabled
      ) {
        val queries = Seq("select id from test_orc", "select name, id from test_orc")

        queries.foreach {
          query =>
            val executedPlan = getExecutedPlan(spark.sql(query))
            val planCondition = executedPlan.exists(_.find(transformerClass.isInstance).isDefined)

            if (shouldFindTransformer) {
              assert(planCondition)
            } else {
              assert(!planCondition)
            }
        }
      }
    }

    testExecPlan(
      "false",
      "true",
      shouldFindTransformer = false,
      classOf[HiveTableScanExecTransformer])
    testExecPlan(
      "false",
      "false",
      shouldFindTransformer = true,
      classOf[HiveTableScanExecTransformer])

    testExecPlan(
      "true",
      "true",
      shouldFindTransformer = false,
      classOf[FileSourceScanExecTransformer])
    testExecPlan(
      "true",
      "false",
      shouldFindTransformer = true,
      classOf[FileSourceScanExecTransformer])
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_orc"),
      ignoreIfNotExists = true,
      purge = false)
  }

  testGluten("avoid unnecessary filter binding for subfield during scan") {
    withSQLConf(
      "spark.sql.hive.convertMetastoreParquet" -> "false",
      "spark.gluten.sql.complexType.scan.fallback.enabled" -> "false") {
      sql("DROP TABLE IF EXISTS test_subfield")
      sql(
        "CREATE TABLE test_subfield (name STRING, favorite_color STRING," +
          " label STRUCT<label_1:STRING, label_2:STRING>) USING hive OPTIONS(fileFormat 'parquet')")
      sql(
        "INSERT INTO test_subfield VALUES('test_1', 'red', named_struct('label_1', 'label-a'," +
          "'label_2', 'label-b'))")
      val df = spark.sql("select * from test_subfield where name='test_1'")
      checkAnswer(df, Seq(Row("test_1", "red", Row("label-a", "label-b"))))
      checkOperatorMatch[HiveTableScanExecTransformer](df)
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("test_subfield"),
      ignoreIfNotExists = true,
      purge = false)
  }

  test("GLUTEN-11062: Supports mixed input format for partitioned Hive table") {
    val hiveClient: HiveClient =
      spark.sharedState.externalCatalog.unwrapped.asInstanceOf[HiveExternalCatalog].client

    withSQLConf("spark.sql.hive.convertMetastoreParquet" -> "false") {
      withTempDir {
        dir =>
          val parquetLoc = s"file:///$dir/test_parquet"
          val orcLoc = s"file:///$dir/test_orc"
          withTable("test_parquet", "test_orc") {
            hiveClient.runSqlHive(s"""create table test_parquet(id int)
                 partitioned by(pid int)
                 stored as parquet location '$parquetLoc'
                 """.stripMargin)
            hiveClient.runSqlHive("insert into test_parquet partition(pid=1) select 2")
            hiveClient.runSqlHive(s"""create table test_orc(id int)
                 partitioned by(pid int)
                 stored as orc location '$orcLoc'
                 """.stripMargin)
            hiveClient.runSqlHive("insert into test_orc partition(pid=2) select 2")
            hiveClient.runSqlHive(
              s"alter table test_parquet add partition (pid=2) location '$orcLoc/pid=2'")
            hiveClient.runSqlHive("alter table test_parquet partition(pid=2) SET FILEFORMAT orc")
            val df = sql("select pid, id from test_parquet order by pid")
            checkAnswer(df, Seq(Row(1, 2), Row(2, 2)))
            checkOperatorMatch[HiveTableScanExecTransformer](df)
          }
      }
    }
  }

  testGluten(
    "GLUTEN-11980: For decimal-key joins, " +
      "if one side falls back to Spark, force fallback the other side") {
    withSQLConf("spark.sql.hive.convertMetastoreOrc" -> "false") {
      withTable("htmp1_wide", "htmp2_wide", "htmp1", "htmp2") {
        // ORC files are written with DECIMAL(38, 18) (Hive's native storage precision).
        // The metastore tables htmp1/htmp2 declare DECIMAL(20, 0) and point to the
        // same ORC files, so the reader must handle a precision/scale mismatch.
        // Selecting only c2 (INT) -> native HiveTableScanExecTransformer.
        // Selecting c3 (TIMESTAMP) in addition -> native validation fails ->
        //   vanilla HiveTableScanExec.
        sql("CREATE TABLE htmp1_wide (c1 DECIMAL(38, 18), c2 INT, c3 TIMESTAMP) STORED AS ORC")
        sql("CREATE TABLE htmp2_wide (c1 DECIMAL(38, 18), c2 INT, c3 TIMESTAMP) STORED AS ORC")
        sql("INSERT INTO htmp1_wide " +
          "SELECT cast(id AS DECIMAL(38, 18)), id % 3, cast(id % 9 AS TIMESTAMP) " +
          "FROM range(1, 101)")
        sql("INSERT INTO htmp2_wide " +
          "SELECT cast(id AS DECIMAL(38, 18)), id % 3, cast(id % 5 AS TIMESTAMP) " +
          "FROM range(1, 101)")
        val loc1 = sql("DESCRIBE FORMATTED htmp1_wide")
          .filter("col_name = 'Location'")
          .select("data_type")
          .collect()(0)
          .getString(0)
        val loc2 = sql("DESCRIBE FORMATTED htmp2_wide")
          .filter("col_name = 'Location'")
          .select("data_type")
          .collect()(0)
          .getString(0)
        sql("CREATE TABLE htmp1 (c1 DECIMAL(20, 0), c2 INT, c3 TIMESTAMP) " +
          s"STORED AS ORC LOCATION '$loc1'")
        sql("CREATE TABLE htmp2 (c1 DECIMAL(20, 0), c2 INT, c3 TIMESTAMP) " +
          s"STORED AS ORC LOCATION '$loc2'")

        // -- SortMergeJoin ------------------------------------------------------------------

        val sql1 =
          "SELECT /*+ MERGE(htmp1) */ htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(
          GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
          GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
          checkAnswer(
            spark.sql(sql1),
            spark.sql(
              "SELECT htmp1_wide.c2 AS 1c2, htmp1_wide.c3 AS 1c3, " +
                "htmp2_wide.c2 AS 2c2, htmp2_wide.c3 AS 2c3 " +
                "FROM htmp1_wide JOIN htmp2_wide ON htmp1_wide.c1 = htmp2_wide.c1")
          )
        }

        val sql2 =
          "SELECT /*+ MERGE(htmp1) */ htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(
          GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
          GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
          checkAnswer(
            spark.sql(sql2),
            spark.sql(
              "SELECT htmp1_wide.c2 AS 1c2, htmp1_wide.c3 AS 1c3, " +
                "htmp2_wide.c2 AS 2c2 " +
                "FROM htmp1_wide JOIN htmp2_wide ON htmp1_wide.c1 = htmp2_wide.c1")
          )
        }

        val sql3 =
          "SELECT /*+ MERGE(htmp1) */ htmp1.c2 AS 1c2, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(
          GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
          GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
          checkAnswer(
            spark.sql(sql3),
            spark.sql(
              "SELECT htmp1_wide.c2 AS 1c2, " +
                "htmp2_wide.c2 AS 2c2, htmp2_wide.c3 AS 2c3 " +
                "FROM htmp1_wide JOIN htmp2_wide ON htmp1_wide.c1 = htmp2_wide.c1")
          )
        }

        // -- ShuffledHashJoin ---------------------------------------------------------------

        val sql4 =
          "SELECT /*+ SHUFFLE_HASH(htmp1) */ htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          checkAnswer(
            spark.sql(sql4),
            spark.sql(
              "SELECT htmp1_wide.c2 AS 1c2, htmp1_wide.c3 AS 1c3, " +
                "htmp2_wide.c2 AS 2c2, htmp2_wide.c3 AS 2c3 " +
                "FROM htmp1_wide JOIN htmp2_wide ON htmp1_wide.c1 = htmp2_wide.c1")
          )
        }

        val sql5 =
          "SELECT /*+ SHUFFLE_HASH(htmp1) */ htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          checkAnswer(
            spark.sql(sql5),
            spark.sql(
              "SELECT htmp1_wide.c2 AS 1c2, htmp1_wide.c3 AS 1c3, " +
                "htmp2_wide.c2 AS 2c2 " +
                "FROM htmp1_wide JOIN htmp2_wide ON htmp1_wide.c1 = htmp2_wide.c1")
          )
        }

        val sql6 =
          "SELECT /*+ SHUFFLE_HASH(htmp1) */ htmp1.c2 AS 1c2, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          checkAnswer(
            spark.sql(sql6),
            spark.sql(
              "SELECT htmp1_wide.c2 AS 1c2, " +
                "htmp2_wide.c2 AS 2c2, htmp2_wide.c3 AS 2c3 " +
                "FROM htmp1_wide JOIN htmp2_wide ON htmp1_wide.c1 = htmp2_wide.c1")
          )
        }

        // -- BroadcastHashJoin --------------------------------------------------------------

        val sql7 =
          "SELECT htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        checkAnswer(
          spark.sql(sql7),
          spark.sql(
            "SELECT htmp1_wide.c2 AS 1c2, htmp1_wide.c3 AS 1c3, " +
              "htmp2_wide.c2 AS 2c2, htmp2_wide.c3 AS 2c3 " +
              "FROM htmp1_wide JOIN htmp2_wide ON htmp1_wide.c1 = htmp2_wide.c1")
        )

        val sql8 =
          "SELECT htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        checkAnswer(
          spark.sql(sql8),
          spark.sql(
            "SELECT htmp1_wide.c2 AS 1c2, htmp1_wide.c3 AS 1c3, " +
              "htmp2_wide.c2 AS 2c2 " +
              "FROM htmp1_wide JOIN htmp2_wide ON htmp1_wide.c1 = htmp2_wide.c1")
        )

        val sql9 =
          "SELECT htmp1.c2 AS 1c2, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        checkAnswer(
          spark.sql(sql9),
          spark.sql(
            "SELECT htmp1_wide.c2 AS 1c2, " +
              "htmp2_wide.c2 AS 2c2, htmp2_wide.c3 AS 2c3 " +
              "FROM htmp1_wide JOIN htmp2_wide ON htmp1_wide.c1 = htmp2_wide.c1")
        )
      }
    }
  }
}
