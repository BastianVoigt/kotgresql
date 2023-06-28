package kodbc.postgresql

data class ErrorMessage(
    val severityLocalized: String,
    val sqlstateCode: String,
    val severity: String,
    val humanReadableErrorMessage: String,
    val details: String?,
    val hint: String?,
    val position: Int?,
    val internalPosition: Int?,
    val internalQuery: String?,
    val where: String?,
    val schemaName: String?,
    val tableName: String?,
    val columnName: String?,
    val dataTypeName: String?,
    val constraintName: String?,
    val file: String?,
    val lineNumber: String?,
    val routine: String?
) {
    class Builder(
        var severityLocalized: String? = null,
        var sqlstateCode: String? = null,
        var severity: String? = null,
        var humanReadableErrorMessage: String? = null,
        var details: String? = null,
        var hint: String? = null,
        var position: Int? = null,
        var internalPosition: Int? = null,
        var internalQuery: String? = null,
        var where: String? = null,
        var schemaName: String? = null,
        var tableName: String? = null,
        var columnName: String? = null,
        var dataTypeName: String? = null,
        var constraintName: String? = null,
        var file: String? = null,
        var lineNumber: String? = null,
        var routine: String? = null
    ) {
        fun build() = ErrorMessage(
            severityLocalized = severityLocalized!!,
            sqlstateCode = sqlstateCode!!,
            severity = severity!!,
            humanReadableErrorMessage = humanReadableErrorMessage!!,
            details = details,
            hint = hint,
            position = position,
            internalPosition = internalPosition,
            internalQuery = internalQuery,
            where = where,
            schemaName = schemaName,
            tableName = tableName,
            columnName = columnName,
            dataTypeName = dataTypeName,
            constraintName = constraintName,
            file = file,
            lineNumber = lineNumber,
            routine = routine
        )
    }
}