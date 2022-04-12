# FAQ: Frequently Asked Questions
<hr>

Q - I get an error like this when first indexing with qbeast following the steps from Quickstart:
  ```
  java.io.IOException: (null) entry in command string: null chmod 0644
  ```
A - You can find the solution [here](https://stackoverflow.com/questions/48010634/why-does-spark-application-fail-with-ioexception-null-entry-in-command-strin/48012285#48012285)
<hr>

Q - I run into an "out or memory error" when indexing with qbeast format.

```bash
java.lang.OutOfMemoryError
	at sun.misc.Unsafe.allocateMemory(Native Method)
	at java.nio.DirectByteBuffer.(DirectByteBuffer.java:127)
	at java.nio.ByteBuffer.allocateDirect(ByteBuffer.java:311)
```

A - Since we process the data per partition, **large partitions can cause the JVM to run out of memory**. 

Try to `repartition` the `DataFrame` before writing on your Spark Application:

```scala
df.repartition(200).write.format("qbeast").option("columnsToIndex", "x,y").save("/tmp/qbeast")
```
<hr>
