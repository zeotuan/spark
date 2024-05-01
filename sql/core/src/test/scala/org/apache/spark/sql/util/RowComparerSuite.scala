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

import java.time.Instant

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row

class RowComparerSuite extends SparkFunSuite {
  test("compare simple equal row") {
    val r1 = Row("a", "b", 4, 1.0, null, BigDecimal.decimal(10.0), Instant.EPOCH, Float.NaN)
    val r2 = Row("a", "b", 4, 1.0, null, BigDecimal.decimal(10.0), Instant.EPOCH, Float.NaN)
    assert(RowComparer.rowEqual(r1, r2))
  }

  test("compare unequal row length") {
    val r1 = Row(1, 2, 3, 4, 5, 6, 7, 8)
    val r2 = Row(1)
    assert(!RowComparer.rowEqual(r1, r2))
  }

  test("compare unequal null") {
    val r1 = Row(null)
    val r2 = Row("b")
    assert(!RowComparer.rowEqual(r1, r2))
  }

  test("compare unequal Double NaN") {
    val r1 = Row(Double.NaN)
    val r2 = Row(Double.MinValue)
    assert(!RowComparer.rowEqual(r1, r2))
  }

  test("compare unequal Float Nan") {
    val r1 = Row(Float.NaN)
    val r2 = Row(Float.MinValue)
    assert(!RowComparer.rowEqual(r1, r2))
  }

  test("compare unequal Instant") {
    val r1 = Row(Instant.EPOCH)
    val r2 = Row(Instant.EPOCH.plusSeconds(200))
    assert(!RowComparer.rowEqual(r1, r2))
  }

  test("compare equal nested Row") {
    val r1 = Row(
      "a",
      Row(
        1,
        Row(
          2.0,
          Row(
            null,
            Row(
              Seq(Row("c"), Row("d")),
              BigDecimal.decimal(1.0),
              Row(Instant.EPOCH)
            )
          )
        )
      )
    )

   val r2 = Row(
     "a",
     Row(
       1,
       Row(
         2.0,
         Row(
           null,
           Row(
             Seq(Row("c"), Row("d")),
             BigDecimal.decimal(1.0),
             Row(Instant.EPOCH)
           )
         )
       )
     )
   )

    assert(RowComparer.rowEqual(r1, r2, 0))
  }

  test("compare unequal nested Row") {
    val r1 = Row(
      "a",
      Row(
        1,
        Row(
          2.0,
          Row(
            null,
            Row(
              Seq(Row("c"), Row("d")),
              BigDecimal.decimal(1.0),
              Row(Instant.EPOCH)
            )
          )
        )
      )
    )
    val r2 = Row("a", Row(1, Row(2.0, Row(null, Row(BigDecimal.decimal(1.0), Row(Instant.EPOCH))))))
    assert(!RowComparer.rowEqual(r1, r2))
  }
}

