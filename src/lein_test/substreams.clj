(ns lein-test.substreams
  (:gen-class)
  (:require [sf.substreams.rpc.v2 :as rpc]
            [sf.substreams.rpc.v2.Stream.client :as stream]
            [protojure.grpc.client.providers.http2 :as grpc.http2]
            [com.example.addressbook.Greeter.client :as greeter]
            [dotenv :refer [env app-env]]
            [clojure.core.async :as async]
            [clojure.java.io]
            [sf.substreams.v1 :as v1]))

(defn slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (with-open [in (clojure.java.io/input-stream x)
              out (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy in out)
    (.toByteArray out)))

(def spkg (v1/pb->Package (slurp-bytes "substream.spkg")))

;(def client @(grpc.http2/connect {:uri "http://localhost:8080"}))
                                  ;:metadata ["foo" "bar"]}))

;@(greeter/Hello client {:name "Janet Johnathan Doe"})

(def client @(grpc.http2/connect {:uri "https://polygon.substreams.pinax.network:443"
                                  :ssl true
                                  :metadata {"authorization" (env "SUBSTREAMS_API_TOKEN")}}))

@(stream/Blocks client (rpc/new-Request {}) (async/chan 100))
  ;; (:gen-class)
  ;; (:require
  ;;  [cheshire.core :as ch]
  ;;  [lein-test.constants :refer [ENTITIES]]
  ;;  [clojure.string :as cstr]
  ;;  [clojure.java.io :as io]
  ;;  [lein-test.spec.action :as action]
  ;;  [honey.sql :as sql]
  ;;  [honey.sql.helpers :as h]
  ;;  [next.jdbc :as jdbc]
  ;;  [lein-test.tables :refer [->action ->triple ->entity ->entity-type ->entity-attribute ->spaces]]
  ;;  [lein-test.db-helpers :refer [nuke-db try-execute create-type-tables make-space-schemas]
  ;;  [sf.substreams.rpc.v2 :as rpc]]))

;; Our grpc connection must pass in the authorization key with the value of our "token" r.metadata_mut().insert("authorization", t.clone());
;; :headers {"Content-Type" "application/grpc-web-text"}
