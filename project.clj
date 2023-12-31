(defproject geo-sink "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                  [cheshire "5.12.0"]
                  [com.github.seancorfield/next.jdbc "1.3.894"]
                  [org.postgresql/postgresql "42.6.0"]
                  [com.github.seancorfield/honeysql "2.4.1078"]
                  [org.clojure/data.json "2.4.0"]
                  [com.zaxxer/HikariCP "5.0.1"]
                  [lynxeyes/dotenv "1.1.0"]

                  [io.github.protojure/core "2.0.1"]
                  [io.github.protojure/grpc-client "2.0.1"]
                  [io.github.protojure/google.protobuf "2.0.0"]]
  :main ^:skip-aot geo-sink.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
