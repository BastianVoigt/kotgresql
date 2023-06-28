package kodbc.postgresql

class SQLException(errorMessage: ErrorMessage) : Exception(errorMessage.humanReadableErrorMessage)