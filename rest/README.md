
# Spark Scala Code - Rest Service Call

```scala
// main code
val spark = SparkSession.builder().appName("rest").master("local").getOrCreate()

disableSSLCertificationVerification()

//
val reqBody = "{}"
val endPoint = "https://xyz.com/getWeatherData"
val headerMap: Map[String, String] =Map(
        ("Authorization", "Bearer "),
        ("Content-Type", "application/json")
)

val url: URL = new URL(endPoint);
val connection: HttpURLConnection = url.openConnection.asInstanceOf[HttpURLConnection]
connection.setRequestMethod("POST")
connection.setConnectTimeout(5000)
connection.setReadTimeout(5000)

//
val map = headerMap
map.foreach(i => {
  connection.setRequestProperty(i._1, i._2)
  println("Key = " + i._1 + "&& value =" + i._2)
})
connection.setUseCaches(false)
connection.setDoOutput(true)
connection.setDoInput(true)
connection.connect()

// write the request body
val dataOutputStream = new DataOutputStream(connection.getOutputStream)
dataOutputStream.writeBytes(reqBody);
dataOutputStream.flush()
dataOutputStream.close()

// read response body
val httpResCode = connection.getResponseCode
println(s"httpResCode => $httpResCode")
// read response body
val inputStream = connection.getInputStream
val content = scala.io.Source.fromInputStream(inputStream).mkString
if (inputStream != null) {
  inputStream.close()
}
connection.disconnect()
println("httpResCode => "+httpResCode, content)
println("httpResBody => "+content)

// All methods

def disableSSLCertificationVerification(): Unit = {
  //
  val trustAllCerts: Array[TrustManager] = getTrustManager
  //
  var sslContext: SSLContext = SSLContext.getInstance("TLS")
  sslContext.init(Array[KeyManager](), trustAllCerts, new SecureRandom())
  HttpsURLConnection.setDefaultSSLSocketFactory(sslContext.getSocketFactory)
  HttpsURLConnection.setDefaultHostnameVerifier(VerifyAllHostNames)
}

object VerifyAllHostNames extends HostnameVerifier {
  override def verify(s: String, sslSession: SSLSession): Boolean = true
}

def getTrustManager = {
  val trustAllCerts = Array[TrustManager](new X509TrustManager() {
    def getAcceptedIssuers: Array[X509Certificate] =
      Array[X509Certificate]()
      @throws[CertificateException]
      override def checkClientTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}

      @throws[CertificateException]
      override def checkServerTrusted(x509Certificates: Array[X509Certificate], s: String): Unit = {}
    })
    trustAllCerts
}
```
