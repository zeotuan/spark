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

import scala.math.abs

import org.apache.spark.sql.Row

private object RowComparer {
  def rowEqual(r1: Row, r2: Row, tol: Double = 0): Boolean = {
    if (tol == 0) {
      return r1 == r2
    }
    if (r1.length != r2.length) {
      return false
    }
    for (i <- 0 until r1.length) {
      val valid = (r1.get(i), r2.get(i)) match {
        case (null, null) => true
        case (null, _) | (_, null) => false
        case (b1: Array[Byte], b2: Array[Byte]) => java.util.Arrays.equals(b1, b2)
        case (f1: Float, f2: Float) =>
          java.lang.Float.isNaN(f1) == java.lang.Float.isNaN(f2) &&
            abs(f1 - f2) <= tol
        case (d1: Double, d2: Double) =>
          java.lang.Double.isNaN(d1) == java.lang.Double.isNaN(d2) &&
            abs(d1 - d2) <= tol
        case (bd1: java.math.BigDecimal, bd2: java.math.BigDecimal) =>
          bd1.subtract(bd2).abs().compareTo(new java.math.BigDecimal(tol)) == -1
        case (t1: java.time.Instant, t2: java.time.Instant) => abs(t1.compareTo(t2)) <= tol
        case (rr1: Row, rr2: Row) => rowEqual(rr1, rr2, tol)
        case (o1, o2) => o1 == o2
      }
      if (!valid) {
        return false
      }
    }
    true
  }
}
