package kotgresql.core

class PostgresErrorResponseException(errorResponse: ErrorResponse) :
  KotgresException(errorResponse.severity + ": " + errorResponse.humanReadableErrorMessage)