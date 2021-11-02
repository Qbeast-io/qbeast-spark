/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.model;

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
