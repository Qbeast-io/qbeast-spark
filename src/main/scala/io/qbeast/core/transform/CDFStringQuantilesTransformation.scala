package io.qbeast.core.transform

case class CDFStringQuantilesTransformation(quantiles: IndexedSeq[String])
    extends CDFQuantilesTransformation {

  override implicit val ordering: Ordering[Any] =
    implicitly[Ordering[String]].asInstanceOf[Ordering[Any]]

  override def mapValue(value: Any): Any = {
    value match {
      case v: String => v
      case null => "null"
      case _ => value.toString
    }
  }

}
