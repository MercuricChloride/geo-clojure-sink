(ns lein-test.core
  (:gen-class)
  (:require [lein-test.cache :refer [cached-actions]]
            [lein-test.populate :refer [actions->db]]
            [lein-test.substreams :as substreams]))

(def start-block 36472424)
(def stop-block 48000000)

(defn handle-args
  [args]
  (let [args (into #{} args)
        from-genesis (get args "--from-genesis")
        populate-cache (get args "--populate-cache")
        from-cache (get args "--from-cache")]
    (when from-genesis
      (println "from-genesis")
      (swap! substreams/current-block (fn [_] (str start-block)))
      (swap! substreams/cursor (fn [_] "")))
    (when populate-cache
      (println "populate-cache")
      (swap! substreams/sink-mode (fn [_] :populate-cache)))
    (when from-cache
      (println "from-cache")
      (doseq [actions cached-actions]
        (actions->db actions))
      (swap! substreams/sink-mode (fn [_] :from-cache)))))

(defn -main
  "The main enchilada that runs when you write lein run"
  [& args]

  (handle-args args)

  (while true
    (println "Starting stream at block #" (str @substreams/current-block))
    (let [client (substreams/spawn-client)]
      (substreams/start-stream client start-block stop-block))))
