(ns lein-test.core
  (:gen-class)
  (:require [lein-test.cache :refer [cached-actions cached-roles-granted
                                     cached-roles-revoked]]
            [lein-test.populate :refer [actions->db role-granted->db
                                        role-revoked->db]]
            [lein-test.substreams :as substreams]))

(def start-block 36472424)
(def stop-block 48000000)


(defn -main
  "The main enchilada that runs when you write lein run"
  [& args]

  (let [args (into #{} args)
        from-cache (get args "--from-cache")
        reset-cursor (get args "--reset-cursor")]

    (when reset-cursor
      (swap! substreams/current-block (fn [_] (str start-block)))
      (swap! substreams/cursor (fn [_] "")))

    (when from-cache
      (println "from-cache")
      (doseq [actions cached-actions]
        (actions->db actions))
      (doseq [roles cached-roles-granted]
        (doseq [role roles]
          (role-granted->db role)))

      (doseq [roles cached-roles-revoked]
        (doseq [role roles]
          (role-revoked->db role))))


    (while (not from-cache)
      (println "from-stream block #" (str @substreams/current-block))
      (let [client (substreams/spawn-client)]
        (substreams/start-stream client start-block stop-block)))))
