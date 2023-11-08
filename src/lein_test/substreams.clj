(ns lein-test.substreams
  (:gen-class)
  (:require [cheshire.core :as ch]
            [clojure.core.async :as async]
            [clojure.string :as string]
            [dotenv :refer [env]]
            [geo.clojure.sink :as geo]
            [lein-test.cache :refer [validate-actions]]
            [lein-test.db-helpers :refer [get-cursor update-cursor]]
            [lein-test.populate :refer [actions->db]]
            [lein-test.utils :refer [decode-base64 ipfs-fetch slurp-bytes
                                     write-file]]
            [protojure.grpc.client.providers.http2 :as grpc.http2]
            [protojure.protobuf :as protojure]
            [sf.substreams.rpc.v2 :as rpc]
            [sf.substreams.rpc.v2.Stream.client :as stream]
            [sf.substreams.v1 :as v1]))

(def current-block (atom (:cursors/block_number (get-cursor))))
(def cursor (atom (:cursors/cursor (get-cursor))))
(def sink-mode (atom :populate-cache))

(defn file-exists? [filepath]
  (.exists (java.io.File. filepath)))

(defn cursor-watcher
  "Watches the cursor for changes and updates the database"
  [key ref old-state new-state]
  (update-cursor new-state @current-block))
(add-watch cursor :watcher cursor-watcher)

(defn take-all [ch f]
  (loop []
    (let [val (async/<!! ch)]
      (if val
        (do
          (f val)
          (recur))
        (println "Channel done processing")))))

(defn empty-output?
  [input]
  (and (empty? (:entries input))
       (empty? (:roles-granted input))
       (empty? (:roles-revoked input))))

(defn format-entry-filename
  [input]
  (let [block (first (string/split (:id input) #"-"))
        index (:index input)
        space (:space input)
        author (:author input)]
    (str block "_" index "_" space "_" author)))

(defn uri-type
  [uri]
  (cond
    (string/starts-with? uri "ipfs://") :ipfs
    (string/starts-with? uri "data:application/json;base64,") :base64
    :else :invalid))

(defmulti uri-data uri-type)

(defmethod uri-data :ipfs
  [uri]
  (let [cid (string/replace-first uri "ipfs://" "")]
    (ipfs-fetch cid 3)))

(defmethod uri-data :base64
  [uri]
  (let [data (string/replace-first uri "data:application/json;base64," "")]
    (decode-base64 data)))

(defmulti process-geo-data (fn [_] @sink-mode))

(defmethod process-geo-data :populate-cache
  [geo-output]
  (let [entries (:entries geo-output)
        roles-granted (:roles-granted geo-output)
        roles-revoked (:roles-revoked geo-output)
        entry-path "new-cache/entries-added/"
        granted-path "new-cache/roles-granted/"
        revoked-path "new-cache/roles-revoked/"
        action-path "new-cache/actions/"]
    (doseq [entry entries]
      (let [entry-filename (format-entry-filename entry)]
        (write-file (str entry-path entry-filename) (protojure/->pb entry))
        (when (not (file-exists? (str action-path entry-filename)))
          (spit (str action-path entry-filename) (uri-data (:uri entry))))))
    (doseq [entry roles-granted]
      (when (not (= :null (:role entry)))
        (write-file (str granted-path (:id entry)) (protojure/->pb entry))))
    (doseq [entry roles-revoked]
      (when (not (= :null (:role entry)))
        (write-file (str revoked-path (:id entry)) (protojure/->pb entry))))))
  
(defmethod process-geo-data :from-cache
  [geo-output]
  (let [entries (:entries geo-output)]
    (when (< 0 (count entries))
     (doseq [entry entries]
       (let [block-number @current-block
             author (:author entry)
             space (:space entry)
             uri (:uri entry)
             uri-data (->> uri
                           uri-data)
             actions (:actions (ch/parse-string uri-data true))
             valid-actions (validate-actions space author block-number actions)]
            (actions->db valid-actions))))))

          ;(actions->db (validate-actions (:actions))))))))
 ;; (let [entry-filename (format-entry-filename entry)
 ;;       entry-filename (str entry-path entry-filename)]
 ;;      (when (file-exists? entry-filename)
 ;;        (log-entry->db entry))
 ;;  (doseq [entry roles-granted]
 ;;    (when (not (= :null (:role entry)))
 ;;      (write-file (str granted-path (:id entry)) (protojure/->pb entry))))
 ;;  (doseq [entry roles-revoked]
 ;;    (when (not (= :null (:role entry)))
 ;;      (write-file (str revoked-path (:id entry)) (protojure/->pb entry)))))

;(geo/pb->RoleGranted (slurp-bytes "./new-cache/roles-granted/36472429-0xa8fe17eb738b8bbeb3f567ad0b3f426d1d8f74af053c3bd63c35e8193f0894aa-4"))

;(geo/pb->EntryAdded (slurp-bytes "./new-cache/entries-added/36472440_0_0x170b749413328ac9a94762031a7a05b00c1d2e34_0x66703c058795b9cb215fbcc7c6b07aee7d216f24"))

    (defn handle-block-scoped-data
      [data]
      (try
        (let [message (:message data)
              block-data (:block-scoped-data message)
              stream-cursor (:cursor block-data)]
          (when block-data
            (let [output (:output block-data)
                  map-output (:map-output output)
                  map-value (:value map-output)
                  geo-output (geo/pb->GeoOutput map-value)
                  block-number (:number (:clock block-data))]
              (if (empty-output? geo-output)
                (when (= (mod block-number 100) 0)
                  (println "Empty block at: " block-number))
                (do
                  (println "Got map output at block:" block-number)
                  (process-geo-data geo-output)))
              (swap! current-block (fn [_] (str block-number)))
              (swap! cursor (fn [_] stream-cursor)))))
        (catch Exception e
          (println "GOT ERROR: \n\n\n\n\n" e))))


    (def spkg (v1/pb->Package (slurp-bytes "geo-substream-v1.0.2.spkg")))

    (defn spawn-client [] @(grpc.http2/connect {:uri "https://polygon.substreams.pinax.network:443"
                                                :ssl true
                                                :idle-timeout 60000
                                                :metadata {"authorization" (env "SUBSTREAMS_API_TOKEN")}}))
    (defn start-stream
      ([client]
       (start-stream client 36472424 48000000))
      ([client start-block stop-block]
       (let [channel (async/chan (async/buffer 10))]

         (stream/Blocks client (rpc/new-Request {:start-block-num (Integer/parseInt @current-block)
                                                 :stop-block-num stop-block
                                                 :start-cursor @cursor
                                                 :modules (:modules spkg)
                                                 :output-module "geo_out"}) channel)
         (take-all channel handle-block-scoped-data))))
