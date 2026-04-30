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
package org.apache.gluten.extension.columnar.heuristic

import org.apache.gluten.extension.columnar.FallbackTags
import org.apache.gluten.extension.columnar.validator.Validator

import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{FileSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, BroadcastQueryStageExec, ShuffleQueryStageExec}
import org.apache.spark.sql.execution.joins.{BroadcastHashJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.types.DecimalType

// Add fallback tags when validator returns negative outcome.
case class AddFallbackTags(validator: Validator) extends Rule[SparkPlan] {
  def apply(plan: SparkPlan): SparkPlan = {
    plan.foreachUp {
      case p if FallbackTags.maybeOffloadable(p) => addFallbackTag(p)
      case _ =>
    }

    plan.foreach(validateJoin)

    plan
  }

  private def addFallbackTag(plan: SparkPlan): Unit = {
    val outcome = validator.validate(plan)
    outcome match {
      case Validator.Failed(reason) =>
        FallbackTags.add(plan, reason)
      case Validator.Passed =>
    }
  }

  /**
   * Traverses the plan tree looking for join nodes (SortMergeJoin, ShuffledHashJoin,
   * BroadcastHashJoin) whose join keys include at least one decimal column.
   *
   * For each such join, delegates to [[setFallbackTagForOtherSide]] to ensure that if one side's
   * scan ([[FileSourceScanExec]] or `HiveTableScanExec`) cannot be offloaded to the native engine,
   * the other side is also forced to fall back. This prevents a decimal-value mismatch that would
   * produce incorrect (typically empty) join results when one side applies Spark's precision
   * coercion and the other side reads raw native values.
   *
   * AdaptiveSparkPlanExec is handled by descending into its `initialPlan`; all other non-join nodes
   * are handled recursively through their children.
   */
  private def validateJoin(plan: SparkPlan): Unit =
    plan match {
      case smj: SortMergeJoinExec
          if (smj.leftKeys ++ smj.rightKeys).exists(_.dataType.isInstanceOf[DecimalType]) =>
        setFallbackTagForOtherSide(smj.left, smj.right)
      case shj: ShuffledHashJoinExec
          if (shj.leftKeys ++ shj.rightKeys).exists(_.dataType.isInstanceOf[DecimalType]) =>
        setFallbackTagForOtherSide(shj.left, shj.right)
      case bhj: BroadcastHashJoinExec
          if (bhj.leftKeys ++ bhj.rightKeys).exists(_.dataType.isInstanceOf[DecimalType]) =>
        setFallbackTagForOtherSide(bhj.left, bhj.right)
      case a: AdaptiveSparkPlanExec =>
        validateJoin(a.initialPlan)
      case _ => plan.children.foreach(validateJoin(_))
    }

  /**
   * Enforces symmetric scan fallback for the two sides of a decimal-key join.
   *
   * When the join key is a decimal type, a native (Velox) scan and a vanilla Spark scan
   * ([[FileSourceScanExec]] or `HiveTableScanExec`) may produce different representations of the
   * same decimal value: the native reader may surface raw uncoerced int128_t values while the
   * vanilla reader applies Spark's precision coercion (returning NULL for out-of-range values). If
   * only one side falls back, the join key values diverge and the join returns 0 rows.
   *
   * This method detects the asymmetric case (exactly one side contains a fallback scan) and adds a
   * fallback tag to the native scan on the other side, so that both sides end up using the same
   * read path.
   *
   * @param leftChild
   *   the left subtree of the join
   * @param rightChild
   *   the right subtree of the join
   */
  private def setFallbackTagForOtherSide(leftChild: SparkPlan, rightChild: SparkPlan): Unit = {
    val leftHasFallbackScan = hasFallbackScan(leftChild)
    val rightHasFallbackScan = hasFallbackScan(rightChild)
    if (leftHasFallbackScan != rightHasFallbackScan) {
      val reason = "asymmetric DataSource scan fallback on " +
        s"decimal join key: left=$leftHasFallbackScan right=$rightHasFallbackScan"
      if (leftHasFallbackScan) {
        addFallbackTagToNativeScan(rightChild, reason)
      } else {
        addFallbackTagToNativeScan(leftChild, reason)
      }
    }
  }

  /**
   * Returns true if the plan node is a DataSource scan that participates in the decimal-key
   * symmetry check.
   *
   * [[FileSourceScanExec]] is matched directly (compile-time dependency available in gluten-core).
   * [[org.apache.spark.sql.hive.execution.HiveTableScanExec]] is matched by simple class name to
   * avoid a direct dependency on `spark-hive` in this module.
   */
  private def isDataSourceScan(plan: SparkPlan): Boolean =
    plan.isInstanceOf[FileSourceScanExec] ||
      plan.getClass.getSimpleName == "HiveTableScanExec"

  /**
   * Returns true if the given plan subtree contains at least one DataSource scan
   * ([[FileSourceScanExec]] or `HiveTableScanExec`) that fails native validation and would fall
   * back to vanilla Spark execution.
   *
   * Transparently descends through AQE stage wrappers ([[ShuffleQueryStageExec]] /
   * [[BroadcastQueryStageExec]]) so that already-materialized stages are inspected correctly during
   * AQE re-planning. For all other node types the check is propagated to children.
   *
   * @param plan
   *   the subtree to inspect
   * @return
   *   true if any tracked scan in the subtree fails validation
   */
  private def hasFallbackScan(plan: SparkPlan): Boolean =
    plan match {
      case q: ShuffleQueryStageExec =>
        hasFallbackScan(q.plan)
      case q: BroadcastQueryStageExec =>
        hasFallbackScan(q.plan)
      case scan if isDataSourceScan(scan) =>
        validator.validate(scan) match {
          case Validator.Passed => false
          case Validator.Failed(_) => true
        }
      case _ => plan.children.exists(hasFallbackScan(_))
    }

  /**
   * Recursively finds every DataSource scan ([[FileSourceScanExec]] or `HiveTableScanExec`) in the
   * given plan subtree that currently passes native validation and adds a fallback tag with the
   * supplied reason.
   *
   * Like [[hasFallbackScan]], this method descends transparently through [[ShuffleQueryStageExec]]
   * and [[BroadcastQueryStageExec]] wrappers so it works correctly in both the initial planning
   * pass and the AQE re-planning pass.
   *
   * @param plan
   *   the subtree to walk
   * @param reason
   *   a human-readable explanation of why the scan is being forced to fall back (logged and
   *   surfaced in Gluten's fallback reporting)
   */
  private def addFallbackTagToNativeScan(plan: SparkPlan, reason: String): Unit =
    plan match {
      case q: ShuffleQueryStageExec =>
        addFallbackTagToNativeScan(q.plan, reason)
      case q: BroadcastQueryStageExec =>
        addFallbackTagToNativeScan(q.plan, reason)
      case scan if isDataSourceScan(scan) =>
        FallbackTags.add(scan, reason)
      case _ => plan.children.foreach(addFallbackTagToNativeScan(_, reason))
    }
}
