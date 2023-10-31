(ns lein-test.substreams
  (:gen-class)
  (:require [sf.substreams.rpc.v2 :as rpc]
            [sf.substreams.rpc.v2.Stream.client :as stream]
            [protojure.grpc.client.providers.http2 :as grpc.http2]
            [geo.clojure.sink :as geo]
            [dotenv :refer [env app-env]]
            [clojure.core.async :as async]
            [clojure.java.io]
            [sf.substreams.v1 :as v1]))

(defn take-all [ch f]
  (async/go (loop []
        (let [val (async/<! ch)]
          (if val
            (do
              (f val)
              (recur))
            (println "Channel closed"))))))

(defn slurp-bytes
  "Slurp the bytes from a slurpable thing"
  [x]
  (with-open [in (clojure.java.io/input-stream x)
              out (java.io.ByteArrayOutputStream.)]
    (clojure.java.io/copy in out)
    (.toByteArray out)))

(defn handle-block-scoped-data
  [data channel]
  (let [message (:message data)
        block-data (:block-scoped-data message)
        output (:output block-data)
        map-output (:map-output output)
        map-value (:value map-output)]
    (try
      (let [entries-added (geo/pb->EntriesAdded map-value)]
        (when (not (= 0 (count (:entries entries-added))))
          (async/put! channel entries-added)))
      (catch Exception e
        ;; (println "Error parsing map output:" e)
        ))))

(def spkg (v1/pb->Package (slurp-bytes "substream.spkg")))

(def client @(grpc.http2/connect {:uri "https://polygon.substreams.pinax.network:443"
                                  :ssl true
                                  :metadata {"authorization" (env "SUBSTREAMS_API_TOKEN")}}))

(let [channel (async/chan) output-channel (async/chan)]
  ; we start a thread to initiate the stream
  (async/thread (stream/Blocks client (rpc/new-Request {:start-block-num 36472424
                                          :stop-block-num 36500000
                                          :modules (:modules spkg)
                                          :output-module "map_entries_added"}) channel))
 ; we then start another thread to filter out the entries that have data
  (async/thread
    (take-all channel #(handle-block-scoped-data % output-channel)))

 ; we then start yet another thread to "process" the entries
  (async/thread
    (take-all output-channel println)))
