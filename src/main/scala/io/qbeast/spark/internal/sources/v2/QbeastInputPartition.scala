/*
 * Copyright 2023 Qbeast Analytics, S.L.
 */
package io.qbeast.spark.internal.sources.v2

import org.apache.spark.sql.connector.read.InputPartition

/**
 * Implementation of [[org.apache.spark.sql.connector.read.InputPartition]].
 */
object QbeastInputPartition extends InputPartition {}
