(ns lein-test.core
  (:gen-class)
  (:require [clojure.spec.alpha :as s]
            [lein-test.cache :refer [cached-actions cached-roles-granted
                                     cached-roles-revoked]]
            [lein-test.constants :refer [default-geo-start-block]]
            [lein-test.db-helpers :refer [reset-geo-db]]
            [lein-test.populate :refer [actions->db role-granted->db
                                        role-revoked->db]]
            [lein-test.substreams :as substreams]))

(s/check-asserts true)


(defn -main
  "The main enchilada that runs when you write lein run"
  [& args]

  (let [args (into #{} args)
        reset-db (get args "--reset-db")
        from-cache (get args "--from-cache")
        reset-cursor (get args "--reset-cursor")]
    
    (when reset-db
      (reset-geo-db)
      (println "Database reset. Exiting.")
      (System/exit 0))

    (when reset-cursor
      (swap! substreams/current-block (fn [_] (str default-geo-start-block)))
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
        (substreams/start-stream client)))))
