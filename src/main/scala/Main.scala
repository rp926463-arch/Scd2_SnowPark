object Main {
  def main(args: Array[String]): Unit = {
    var key_obj = new PrivateKeyReader
    val session = key_obj.get_session_object()

    var scd_obj = new Scd2Process(session, "STAGING.STORES", "FINAL.STORES")
    var error_dict = scd_obj.run(Array("store_id")
      ,"start_date"
      ,"end_date"
      ,"active_flag"
      ,"DELTA"
      ,"2022-10-26"
      ,"9999-12-31")

    println(error_dict)
  }
}