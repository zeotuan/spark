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

case class TestCase(strCol: String, intCol: Int, doubleCol: Double, arrCol: Seq[Int] = Nil)
class DatasetComparerSuite extends SparkFunSuite with SharedSparkSession {
  import testImplicits._
  test("compare unequal TestCase Dataset with different rows") {

    val sourceDS = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0),
      TestCase("bob", 1, 1.0),
      TestCase("li", 49, 1.0),
      TestCase("alice", 5, 1.2)
    ).toDS()

    val expectedDS = Seq(
      TestCase("juanisareallygoodguythatilikealotNOT", 5, 5.0),
      TestCase("frank", 10, 2.9),
      TestCase("li", 49, 4.2),
      TestCase("lucy", 5, 1.0)
    ).toDS()

    val e = intercept[SparkAssertionException] {
      DatasetComparer.assertDatasetEquality(sourceDS, expectedDS)
    }
    assert(e.getMessage.contains("DIFFERENT_ROWS"))
  }

  test("compare equal Dataset that have ArrayType columns") {
    val sourceDS = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDS()

    val expectedDS = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDS()
    expectedDS.toDF().first()

    DatasetComparer.assertDatasetEquality(sourceDS, expectedDS)
  }

  test("compare unequal Dataset that have ArrayType columns") {
    val sourceDS = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(2, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDS()

    val expectedDS = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDS()

    DatasetComparer.assertDatasetEquality(sourceDS, expectedDS)
  }

  test("compare DataFrames that have unequal schema") {
    val sourceDF = Seq(1, 5).toDF()

    val expectedDF = Seq((1, "word"), (5, "word")).toDF()

    val e = intercept[SparkAssertionException] {
      DatasetComparer.assertDatasetEquality(sourceDF, expectedDF)
    }

    assert(e.getMessage.contains("DIFFERENT_SCHEMA"))
  }

  test("compare unequal DataFrames that have ArrayType columns") {
    val sourceDF = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(2, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDF()

    val expectedDF = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDF()


    val e = intercept[SparkAssertionException] {
      DatasetComparer.assertDatasetEquality(sourceDF, expectedDF)
    }

    assert(e.getMessage.contains("DIFFERENT_ROWS"))
  }

  test("compare dataset ignore ordering") {
    val sourceDS = Seq(
      TestCase("alice", 5, 1.2, Seq(1)),
      TestCase("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(2, 2, 3, 2, 3)),
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4))
    ).toDS()

    val expectedDS = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDS()
    DatasetComparer.assertDatasetEquality(sourceDS, expectedDS)
  }

  test("compare inorder dataset without ignore ordering") {
    val sourceDS = Seq(
      TestCase("alice", 5, 1.2, Seq(1)),
      TestCase("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(2, 2, 3, 2, 3)),
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4))
    ).toDS()

    val expectedDS = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDS()

    val e = intercept[SparkAssertionException] {
      DatasetComparer.assertDatasetEquality(sourceDS, expectedDS)
    }

    assert(e.getMessage.contains("DIFFERENT_ROWS"))
  }

  test("compare dataframe ignore ordering") {
    val sourceDF = Seq(
      TestCase("alice", 5, 1.2, Seq(1)),
      TestCase("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(2, 2, 3, 2, 3)),
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4))
    ).toDF()

    val expectedDF = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDF()
    DatasetComparer.assertDatasetEquality(sourceDF, expectedDF)
  }

  test("compare inorder dataframe without ignore ordering") {
    val sourceDF = Seq(
      TestCase("alice", 5, 1.2, Seq(1)),
      TestCase("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(2, 2, 3, 2, 3)),
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4))
    ).toDF()

    val expectedDF = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDF()

    val e = intercept[SparkAssertionException] {
      DatasetComparer.assertDatasetEquality(sourceDF, expectedDF, orderedComparison = true)
    }

    assert(e.getMessage.contains("DIFFERENT_ROWS"))
  }

  test("compare dataset with different row count") {
    val sourceDS = Seq(
      TestCase("alice", 5, 1.2, Seq(1)),
      TestCase("li", 49, 1.0, Seq(3, 5, 6, 3, 4)),
    ).toDS()

    val expectedDS = Seq(
      TestCase("juanisareallygoodguythatilikealotOK", 5, 5.0, Seq(1, 2, 3, 4)),
      TestCase("bob", 1, 1.0, Seq(1, 2, 3, 2, 3)),
      TestCase("li", 49, 1.0, Seq(1, 5, 6, 3, 4)),
      TestCase("alice", 5, 1.2, Seq(1))
    ).toDS()

    val e = intercept[SparkAssertionException] {
      DatasetComparer.assertDatasetEquality(sourceDS, expectedDS)
    }

    assert(e.getMessage.contains("DIFFERENT_ROW_COUNT"))

  }
}