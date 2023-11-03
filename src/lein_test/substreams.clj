(ns lein-test.substreams
  (:gen-class)
  (:require [sf.substreams.rpc.v2 :as rpc]
            [sf.substreams.rpc.v2.Stream.client :as stream]
            [protojure.grpc.client.providers.http2 :as grpc.http2]
            [geo.clojure.sink :as geo]
            [dotenv :refer [env app-env]]
            [clojure.core.async :as async]
            [clojure.java.io]
            [clojure.string :as string]
            [sf.substreams.v1 :as v1]))


(defn ipfs-fetch
  ([cid]
   (when (not (nil? cid))
     (slurp (str "https://ipfs.network.thegraph.com/api/v0/cat?arg=" cid))))
  ([cid max-failures]
   (ipfs-fetch cid 0 max-failures))
  ([cid retry-count max-failures]
   (when (= retry-count max-failures)
    (throw (Exception. "Failed to fetch the cid from ipfs too many times")))
   (try
     (ipfs-fetch cid)
     (catch java.io.IOException e
       (println (str "Failed to fetch data! \n Sleeping for " (* 10 retry-count) "seconds"))
       (Thread/sleep (* 10000 retry-count))
       (ipfs-fetch cid (inc retry-count) max-failures)))))

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
      (let [geo-output (geo/pb->GeoOutput map-value)]
        (when (not (and 
                    (empty? (:entries geo-output)) 
                    (empty? (:roles-granted geo-output)) 
                    (empty? (:roles-granted geo-output))))
              (async/put! channel geo-output)))
      (catch Exception e))))
        ;; (println "Error parsing map output:" e)
        
(defn uri->cid
 [uri]
 (cond
  (string/starts-with? uri "ipfs://") (string/replace-first uri "ipfs://" "")
  (string/starts-with? uri "data:application/json;base64") nil
  :else (println (str "Invalid URI" uri))))

(defn fetch-entries-json
  [entries]
  (->> entries
      (map #(ipfs-fetch (uri->cid (:uri %))))
      (into [])))

(defn process-geo-data
 [geo-output]
 (let [ entries (:entries geo-output)
        roles-granted (:roles-granted geo-output)
        roles-revoked (:roles-revoked geo-output)]
  (println (:id (first entries)))
  (println (fetch-entries-json entries))))
  

(def spkg (v1/pb->Package (slurp-bytes "geo-substream-v1.0.2.spkg")))

(def client @(grpc.http2/connect {:uri "https://polygon.substreams.pinax.network:443"
                                  :ssl true
                                  :metadata {"authorization" (env "SUBSTREAMS_API_TOKEN")}}))

(let [channel (async/chan) output-channel (async/chan)]
  ; we start a thread to initiate the stream
  (async/thread (stream/Blocks client (rpc/new-Request {:start-block-num 36472424
                                                        :stop-block-num 36475834
                                                        :modules (:modules spkg)
                                                        :output-module "geo_out"}) channel))
 ; we then start another thread to filter out the entries that have data
  (async/thread
    (take-all channel #(handle-block-scoped-data % output-channel)))

 ; we then start yet another thread to "process" the entries
  (async/thread
    (take-all output-channel process-geo-data)))
