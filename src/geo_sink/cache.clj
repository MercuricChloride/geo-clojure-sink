(ns geo-sink.cache
  (:require [cheshire.core :as ch]
            [clojure.java.io :as io]
            [clojure.string :as string]
            [geo-sink.constants :refer [cache-cursor-file]]
            [geo-sink.spec.action :as action]
            [geo-sink.utils :refer [slurp-bytes write-file]]
            [geo.clojure.sink :as geo]))

(defn format+filter-actions
  ([actions]
   (filter #(action/valid-action? %) actions))
  ([space author block-number proposal-name actions]
   (println "space:" space)
   (println "author:" author)
   (println "block-number:" block-number)
   (->> (filter #(action/valid-action? %) actions)
        (map #(assoc % :space space :author author :block-number block-number :proposal-name proposal-name)))))

(defn json->actions
  ([path]
   (let [json (slurp path)]
     (format+filter-actions ((ch/parse-string json true) :actions))))
  ([prefix path]
   (let [json (slurp (str prefix path))]
     (format+filter-actions ((ch/parse-string json true) :actions))))
  ([prefix path space author block-number]
   (let [json (slurp (str prefix path))
         json (ch/parse-string json true)
         actions (:actions json)
         proposal-name (:name json)]
        (format+filter-actions space author block-number proposal-name actions))))

(defn sort-files [files]
  (sort-by (juxt :block :index) files))

(defn- extract-file-meta [filename]
  (let [parts (string/split filename #"_")]
    {:block (Integer/parseInt (get parts 0))
     :index (Integer/parseInt (get parts 1))
     :space (.toLowerCase (get parts 2))
     :author (get parts 3)
     :filename filename}))


(defn write-cursor-cache-file [block-number cursor]
  (write-file cache-cursor-file (str "{\"block_number\": \"" block-number "\", \"cursor\": \"" cursor "\"}")))



(def cached-roles-granted (->> (io/file "./cache/roles-granted/")
                           file-seq
                           rest
                           (map #(geo/pb->RoleGranted (slurp-bytes %)))
                           (filter #(not (= (:role %) :null)))))

(def cached-roles-revoked (->> (io/file "./cache/roles-revoked/")
                               file-seq
                               rest
                               (map #(geo/pb->RoleRevoked (slurp-bytes %)))
                               (filter #(not (= (:role %) :null)))))

(def cached-actions (->> (io/file "./cache/actions/")
                         file-seq
                         rest
                         (map #(string/replace % #"./cache/actions/" ""))
                         (map extract-file-meta)
                         sort-files
                         (map #(json->actions "./cache/actions/" (:filename %) (:space %) (:author %) (:block %)))))
