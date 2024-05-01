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
package org.apache.spark.sql.util

import org.apache.spark.{SparkAssertionException, SparkFunSuite}
import org.apache.spark.sql.test.SharedSparkSession

class DataframeComparerSuite extends SparkFunSuite with SharedSparkSession {
  import testImplicits._

  test("compare DataFrames that have unequal schema") {
    val sourceDF = Seq(1, 5).toDF()

    val expectedDF = Seq((1, "word"), (5, "word")).toDF()

    val e = intercept[SparkAssertionException] {
      DataFrameComparer.assertDataFrameEquality(sourceDF, expectedDF)
    }

    assert(e.getMessage.contains("DIFFERENT_SCHEMA"))
  }

  test("compare unequal DataFrames that have ArrayType columns") {
    val sourceDF = Seq(
      ("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      ("bob", 1, 1.0, Seq(2, 2, 3, 2, 3)),
      ("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
      ("alice", 5, 1.2, Seq(1))
    ).toDF()

    val expectedDF = Seq(
      ("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      ("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      ("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      ("alice", 5, 1.2, Seq(1))
    ).toDF()


    val e = intercept[SparkAssertionException] {
      DataFrameComparer.assertDataFrameEquality(sourceDF, expectedDF)
    }

    assert(e.getMessage.contains("DIFFERENT_ROWS"))
  }

  test("compare dataframe ignore ordering") {
    val sourceDF = Seq(
      ("alice", 5, 1.2, Seq(1)),
      ("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
      ("bob", 1, 1.0, Seq(2, 2, 3, 2, 3)),
      ("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4))
    ).toDF()

    val expectedDF = Seq(
      ("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      ("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      ("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      ("alice", 5, 1.2, Seq(1))
    ).toDF()
    DataFrameComparer.assertDataFrameEquality(sourceDF, expectedDF)
  }

  test("compare inorder dataframe without ignore ordering") {
    val sourceDF = Seq(
      ("alice", 5, 1.2, Seq(1)),
      ("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
      ("bob", 1, 1.0, Seq(2, 2, 3, 2, 3)),
      ("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4))
    ).toDF()

    val expectedDF = Seq(
      ("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      ("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      ("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      ("alice", 5, 1.2, Seq(1))
    ).toDF()

    val e = intercept[SparkAssertionException] {
      DataFrameComparer.assertDataFrameEquality(sourceDF, expectedDF, orderedComparison = true)
    }

    assert(e.getMessage.contains("DIFFERENT_ROWS"))
  }

  test("compare dataframe with different row count") {
    val sourceDS = Seq(
      ("alice", 5, 1.2, Seq(1)),
      ("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
    ).toDF()

    val expectedDS = Seq(
      ("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      ("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      ("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      ("alice", 5, 1.2, Seq(1))
    ).toDF()

    val e = intercept[SparkAssertionException] {
      DatasetComparer.assertDatasetEquality(sourceDS, expectedDS)
    }

    assert(e.getMessage.contains("DIFFERENT_ROW_COUNT"))

  }
}