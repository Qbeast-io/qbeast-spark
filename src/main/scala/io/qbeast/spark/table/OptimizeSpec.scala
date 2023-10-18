/*
 * Copyright 2021 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.table

/**
 * The optimize operation specification.
 *
 * @param files the files to optimize
 * @param cubes te cubes to optimize
 */
case class OptimizeSpec(
    files: FilesToOptimize = FilesToOptimize.None,
    cubes: CubesToOptimize = CubesToOptimize.None)
