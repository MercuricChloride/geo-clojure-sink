(ns lein-test.core
  (:gen-class)
  (:require
   [cheshire.core :as ch]
   [clojure.spec.alpha :as s]
   [clojure.core.reducers :as r]
   [lein-test.spec.action :as action]
   [lein-test.spec.action-type :as at]))

(defn -main
  "I don't do a whole lot ... yet."
  [& args]
  (println "Hello, World!"))

(defn- validate-actions [actions]
  (filter #(action/valid-action? %) actions))

(defn- json->actions [path]
  (let [json (slurp path)]
    (validate-actions ((ch/parse-string json true) :actions))))

(def actions (json->actions "geo-data.json"))

(defn unique-entities
  "Returns a hash set of unique entities from a collection of maps"
  ([coll key] (reduce #(unique-entities %1 %2 key) #{} coll))  ; if a collection is provided, reduce over it
  ([acc curr key] (conj acc (key curr))))   ; if two arguments are provided, assume it's the reducing step

(def valid-actions (validate-actions actions))

(defn repeat-vector [v n]
  (vec (take n (cycle v))))

(def long-actions (repeat-vector valid-actions 2000))

(defn retry-future
  "Tries to execute a future n times, backing for 5 seconds longer each time it returns false"
  [f n]
  (loop
   [tries 0]
    (when (> n tries)
      (Thread/sleep (* tries 5000))
      (let [val @(f)]
        (if val
          val
            (recur (inc tries)))))))

(defmacro try3 [future]
  `(retry-future (fn [] ~future) 3))

(defn handle-execute [input]
  (let [dependencies (:dependencies input) handler (:handler input)]
    (future
      (try3 dependencies)
      @handler)))

(time (doall (map #(deref % 5000 "timed out") (map #(handle-execute (at/execute %)) (filter #(at/sink-action %) long-actions)))))

(def unique-actions (concat
                     (unique-entities valid-actions :entityId)
                     (unique-entities valid-actions :attributeId)))
