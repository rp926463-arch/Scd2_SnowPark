import com.snowflake.snowpark.Session

class PrivateKeyReader {

  def get_session_object(): Session = {
    var sfOptions = Map(
      "url" -> "bypgtvv-tw48419.snowflakecomputing.com",
      "user" -> "****",
      "password" -> "****",
      "role" -> "accountadmin",
      "warehouse" -> "MY_WH",
      "database" -> "TEST_DB",
      "schema" -> "TEST_SCHEMA"
    )
    val session = Session.builder.configs(sfOptions).create
    session
  }

}
