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

import scala.reflect.ClassTag

import org.apache.spark.SparkAssertionException
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col

object DatasetComparer {
  def assertDatasetEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      ignoreNullable: Boolean = false,
      orderedComparison: Boolean = false,
      primaryKeys: Seq[String] = Nil,
      equals: (T, T) => Boolean = (t1: T, t2: T) => t1 == t2): Unit = {


    SchemaComparer.assertSchemaEqual(actualDS.schema, expectedDS.schema, ignoreNullable)
    assertDatasetContentEquality(
      orderColumns(actualDS, expectedDS), expectedDS, equals, orderedComparison, primaryKeys
    )
  }

  private def sortDataset[T](ds: Dataset[T], primaryKeys: Seq[String]): Dataset[T] = {
    val sortCols = if (primaryKeys.nonEmpty) {
      primaryKeys
    } else {
      ds.columns.toIndexedSeq
    }
    ds.sort(sortCols.map(col).toIndexedSeq: _*)
  }

  /**
   *  order ds1 column according to ds2 column order
   *  */
  private def orderColumns[T](ds1: Dataset[T], ds2: Dataset[T]): Dataset[T] = {
    ds1.select(ds2.columns.map(col).toIndexedSeq: _*).as[T](ds2.encoder)
  }

  private def assertDatasetContentEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean,
      orderedComparison: Boolean,
      primaryKeys: Seq[String]): Unit = {
    if (orderedComparison) {
      assertDatasetContentEquality(
        sortDataset(actualDS, primaryKeys),
        sortDataset(expectedDS, primaryKeys),
        equals
      )
    } else {
      assertDatasetContentEquality(actualDS, expectedDS, equals)
    }
  }


  def assertDatasetContentEquality[T: ClassTag](
      actualDS: Dataset[T],
      expectedDS: Dataset[T],
      equals: (T, T) => Boolean): Unit = {

    try {
      val actualRDD = actualDS.rdd.cache()
      val expectedRDD = expectedDS.rdd.cache()

      val actualCount = actualRDD.count()
      val expectedCount = expectedRDD.count()
      if ( actualCount !=  expectedCount) {
        throw new SparkAssertionException(
          "DIFFERENT_ROW_COUNT",
          Map(
            "expectedCount" -> expectedCount.toString,
            "actualCount" -> actualCount.toString)
        )
      }

      val unequalRDD = actualRDD.zip(expectedRDD).filter { case (o1: T, o2: T) => !equals(o1, o2) }
      if (!unequalRDD.isEmpty()) {
        throw new SparkAssertionException(
          "DIFFERENT_ROWS",
          Map(
            "expectedData" -> expectedRDD.take(10).mkString("[", ", ", "]"),
            "actualData" -> actualRDD.take(10).mkString("[", ", ", "]"))
        )
      }
    } finally {
      actualDS.rdd.unpersist()
      expectedDS.rdd.unpersist()
    }
  }
}
