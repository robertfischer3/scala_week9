/** Configuration utils
  */

package org.cscie88c.config

import pureconfig.{ConfigReader, ConfigSource}
import scala.reflect.ClassTag

// config classes
case class CookieSettings(domain: String, path: String, ttl: Int)
case class SignatureSettings(pkfile: String, keyPairId: String)
case class AppSettings(cookie: CookieSettings, signature: SignatureSettings)

object ConfigUtils {

  /** loads a configuration case class
    */
  def loadAppConfig[A: ConfigReader: ClassTag](path: String): A = {
    ConfigSource.default.at(path).loadOrThrow[A]
  }
}
