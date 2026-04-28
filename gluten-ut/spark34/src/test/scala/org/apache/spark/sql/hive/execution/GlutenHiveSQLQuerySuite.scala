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
import org.apache.gluten.execution.{BroadcastHashJoinExecTransformerBase, ShuffledHashJoinExecTransformerBase, SortMergeJoinExecTransformerBase}

import org.apache.spark.SparkConf
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
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
      withTable("htmp1", "htmp2") {
        // Both tables: Hive ORC with a DECIMAL join key, an INT column, and a TIMESTAMP column.
        // Selecting only c2 (INT) -> native HiveTableScanExecTransformer.
        // Selecting c3 (TIMESTAMP) in addition -> native validation fails ->
        //   vanilla HiveTableScanExec.
        sql("CREATE TABLE htmp1 (c1 DECIMAL(20, 0), c2 INT, c3 TIMESTAMP) STORED AS ORC")
        sql("CREATE TABLE htmp2 (c1 DECIMAL(20, 0), c2 INT, c3 TIMESTAMP) STORED AS ORC")
        sql("INSERT INTO htmp1 SELECT id, id % 3, cast(id % 9 as timestamp) FROM range(1, 101)")
        sql("INSERT INTO htmp2 SELECT id, id % 3, cast(id % 5 as timestamp) FROM range(1, 101)")

        // -- SortMergeJoin ------------------------------------------------------------------

        // Both sides select TIMESTAMP -> both scans fall back to vanilla HiveTableScanExec ->
        // symmetric -> native SMJ.
        val sql1 =
          "SELECT /*+ MERGE(htmp1) */ htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(
          GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
          GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
          val df = spark.sql(sql1)
          df.collect()
          val plan = getExecutedPlan(df)
          assert(plan.count(_.isInstanceOf[SortMergeJoinExec]) == 0)
          assert(plan.count(_.isInstanceOf[SortMergeJoinExecTransformerBase]) == 1)
          assert(plan.count(_.isInstanceOf[HiveTableScanExec]) == 2)
          assert(plan.count(_.isInstanceOf[HiveTableScanExecTransformer]) == 0)
        }

        // Left side falls back: htmp1 selects TIMESTAMP -> vanilla on left, native on right.
        // Asymmetry -> fallback tag added to right -> both scans vanilla -> native SMJ.  100 rows.
        val sql2 =
          "SELECT /*+ MERGE(htmp1) */ htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(
          GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
          GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
          val df = spark.sql(sql2)
          val rows = df.collect()
          assert(rows.length == 100, s"sql2: expected 100 rows but got ${rows.length}")
          val plan = getExecutedPlan(df)
          assert(plan.count(_.isInstanceOf[SortMergeJoinExec]) == 0)
          assert(plan.count(_.isInstanceOf[SortMergeJoinExecTransformerBase]) == 1)
          assert(plan.count(_.isInstanceOf[HiveTableScanExec]) == 2)
          assert(plan.count(_.isInstanceOf[HiveTableScanExecTransformer]) == 0)
        }

        // Right side falls back: htmp2 selects TIMESTAMP -> native on left, vanilla on right.
        // Asymmetry -> fallback tag added to left -> both scans vanilla -> native SMJ.  100 rows.
        val sql3 =
          "SELECT /*+ MERGE(htmp1) */ htmp1.c2 AS 1c2, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(
          GlutenConfig.COLUMNAR_FORCE_SHUFFLED_HASH_JOIN_ENABLED.key -> "false",
          GlutenConfig.COLUMNAR_SHUFFLED_HASH_JOIN_ENABLED.key -> "false") {
          val df = spark.sql(sql3)
          val rows = df.collect()
          assert(rows.length == 100, s"sql3: expected 100 rows but got ${rows.length}")
          val plan = getExecutedPlan(df)
          assert(plan.count(_.isInstanceOf[SortMergeJoinExec]) == 0)
          assert(plan.count(_.isInstanceOf[SortMergeJoinExecTransformerBase]) == 1)
          assert(plan.count(_.isInstanceOf[HiveTableScanExec]) == 2)
          assert(plan.count(_.isInstanceOf[HiveTableScanExecTransformer]) == 0)
        }

        // -- ShuffledHashJoin ---------------------------------------------------------------

        // Both sides native.
        val sql4 =
          "SELECT /*+ SHUFFLE_HASH(htmp1) */ htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val df = spark.sql(sql4)
          df.collect()
          val plan = getExecutedPlan(df)
          assert(plan.count(_.isInstanceOf[ShuffledHashJoinExec]) == 0)
          assert(plan.count(_.isInstanceOf[ShuffledHashJoinExecTransformerBase]) == 1)
          assert(plan.count(_.isInstanceOf[HiveTableScanExec]) == 2)
          assert(plan.count(_.isInstanceOf[HiveTableScanExecTransformer]) == 0)
        }

        // Left side falls back -> asymmetry -> both scans vanilla -> native SHJ.  100 rows.
        val sql5 =
          "SELECT /*+ SHUFFLE_HASH(htmp1) */ htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val df = spark.sql(sql5)
          val rows = df.collect()
          assert(rows.length == 100, s"sql5: expected 100 rows but got ${rows.length}")
          val plan = getExecutedPlan(df)
          assert(plan.count(_.isInstanceOf[ShuffledHashJoinExec]) == 0)
          assert(plan.count(_.isInstanceOf[ShuffledHashJoinExecTransformerBase]) == 1)
          assert(plan.count(_.isInstanceOf[HiveTableScanExec]) == 2)
          assert(plan.count(_.isInstanceOf[HiveTableScanExecTransformer]) == 0)
        }

        // Right side falls back -> asymmetry -> both scans vanilla -> native SHJ.  100 rows.
        val sql6 =
          "SELECT /*+ SHUFFLE_HASH(htmp1) */ htmp1.c2 AS 1c2, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "-1") {
          val df = spark.sql(sql6)
          val rows = df.collect()
          assert(rows.length == 100, s"sql6: expected 100 rows but got ${rows.length}")
          val plan = getExecutedPlan(df)
          assert(plan.count(_.isInstanceOf[ShuffledHashJoinExec]) == 0)
          assert(plan.count(_.isInstanceOf[ShuffledHashJoinExecTransformerBase]) == 1)
          assert(plan.count(_.isInstanceOf[HiveTableScanExec]) == 2)
          assert(plan.count(_.isInstanceOf[HiveTableScanExecTransformer]) == 0)
        }

        // -- BroadcastHashJoin --------------------------------------------------------------

        // Both sides native.
        val sql7 =
          "SELECT htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        val df7 = spark.sql(sql7)
        df7.collect()
        val plan7 = getExecutedPlan(df7)
        assert(plan7.count(_.isInstanceOf[BroadcastHashJoinExec]) == 0)
        assert(plan7.count(_.isInstanceOf[BroadcastHashJoinExecTransformerBase]) == 1)
        assert(plan7.count(_.isInstanceOf[HiveTableScanExec]) == 2)
        assert(plan7.count(_.isInstanceOf[HiveTableScanExecTransformer]) == 0)

        // Left side falls back -> asymmetry -> both scans vanilla -> native BHJ.  100 rows.
        val sql8 =
          "SELECT htmp1.c2 AS 1c2, htmp1.c3 AS 1c3, " +
            "htmp2.c2 AS 2c2 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        val df8 = spark.sql(sql8)
        val rows8 = df8.collect()
        assert(rows8.length == 100, s"sql8: expected 100 rows but got ${rows8.length}")
        val plan8 = getExecutedPlan(df8)
        assert(plan8.count(_.isInstanceOf[BroadcastHashJoinExec]) == 0)
        assert(plan8.count(_.isInstanceOf[BroadcastHashJoinExecTransformerBase]) == 1)
        assert(plan8.count(_.isInstanceOf[HiveTableScanExec]) == 2)
        assert(plan8.count(_.isInstanceOf[HiveTableScanExecTransformer]) == 0)

        // Right side falls back -> asymmetry -> both scans vanilla -> native BHJ.  100 rows.
        val sql9 =
          "SELECT htmp1.c2 AS 1c2, " +
            "htmp2.c2 AS 2c2, htmp2.c3 AS 2c3 FROM htmp1 JOIN htmp2 ON htmp1.c1 = htmp2.c1"
        val df9 = spark.sql(sql9)
        val rows9 = df9.collect()
        assert(rows9.length == 100, s"sql9: expected 100 rows but got ${rows9.length}")
        val plan9 = getExecutedPlan(df9)
        assert(plan9.count(_.isInstanceOf[BroadcastHashJoinExec]) == 0)
        assert(plan9.count(_.isInstanceOf[BroadcastHashJoinExecTransformerBase]) == 1)
        assert(plan9.count(_.isInstanceOf[HiveTableScanExec]) == 2)
        assert(plan9.count(_.isInstanceOf[HiveTableScanExecTransformer]) == 0)
      }
    }
  }
}
