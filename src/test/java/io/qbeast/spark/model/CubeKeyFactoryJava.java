/*
 * Copyright 2021 Qbeast Analytics, S.L.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.qbeast.spark.model;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * Cube Key builder
 */
public abstract class CubeKeyFactoryJava {

    /**
     * Method to create the cube key for a certain point in the space
     *
     * @param dime coordinates of each dimension
     * @param times level of the tree
     * @param offset character offset
     * @return a string identifier
     */
    public static String createCubeKey(double[] dime, int times, int offset) {
        if (times < 31) {
            final long mul = 1L << times;
            long[] rounded = new long[dime.length];
            for (int i = 0; i < dime.length; i++) {
                rounded[i] = (long) (dime[i] * mul);
            }
            char[] result = new char[times];
            int pos = 1;
            result[0] = '+';
            for (int i = times - 1; i > 0; i--) {
                long mask = 1L << i;
                long cell = 0;
                for (int j = 0; j < dime.length; j++) {
                    cell = (cell | ((rounded[j] & mask) >> i) << j);
                }
                result[pos++] = (char) (cell + offset);
            }
            return String.valueOf(result);
        } else {
            final BigDecimal mul = new BigDecimal(BigInteger.ONE.shiftLeft(times));
            BigInteger[] rounded = new BigInteger[dime.length];
            for (int i = 0; i < dime.length; i++) {
                rounded[i] = BigDecimal.valueOf(dime[i]).multiply(mul).toBigInteger();
            }
            char[] result = new char[times];
            int pos = 1;
            result[0] = '+';
            for (int i = times - 1; i > 0; i--) {
                BigInteger mask = BigInteger.ONE.shiftLeft(i);
                int cell = 0;
                for (int j = 0; j < dime.length; j++) {
                    cell = (cell | rounded[j].and(mask).shiftRight(i).shiftLeft(j).intValue());
                }
                result[pos++] = (char) (cell + offset);
            }
            return String.valueOf(result);
        }
    }
}
