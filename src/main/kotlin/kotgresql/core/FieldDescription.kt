package kotgresql.core

data class FieldDescription(
  val name: String,
  val tableObjectId: Int,
  val columnAttributeNumber: Short,
  val dataTypeObjectId: Int,
  val dataTypeSize: Short,
  val typeModifier: Int,
  val formatCode: FormatCode
)