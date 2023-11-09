(ns lein-test.core
  (:gen-class)
  (:require [clojure.spec.alpha :as s]
            [dotenv :refer [env]]
            [lein-test.cache :refer [cached-actions cached-roles-granted
                                     cached-roles-revoked]]
            [lein-test.constants :refer [cache-action-path cache-entry-path
                                         cache-granted-path cache-revoked-path
                                         default-geo-start-block]]
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

    ;; Check for required environment variables
    (let [env-vars ["SUBSTREAMS_API_TOKEN" "SUBSTREAMS_ENDPOINT" "PGDATABASE" "PGUSER" "PGPASSWORD" "PGPORT" "PGHOST"]]
      (doseq [env-var env-vars]
        (when (not (env env-var))
          (throw (Exception. (str "Environment variable " env-var " is not defined"))))))
    
    ;; Create required cache directories
    (doseq [path [cache-entry-path cache-granted-path cache-revoked-path cache-action-path]]
      (when-not (.exists (java.io.File. path))
        (.mkdirs (java.io.File. path))))

    ;; --reset-db flag to clear and bootstrap the database with some fundamental entities
    (when reset-db
      (reset-geo-db)
      (println "Database reset. Exiting.")
      (System/exit 0))

    ;; --reset-cursor flag to reset the cursor to the default start block
    (when reset-cursor
      (swap! substreams/current-block (fn [_] (str default-geo-start-block)))
      (swap! substreams/cursor (fn [_] "")))

    ;; --from-cache flag to populate the database from the cache
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


    ;; Start streaming the substreams client and populating the cache when --from-cache is not set (TODO: Explore auto-streaming after cache is populated)
    (while (not from-cache)
      (println "from-stream block #" (str @substreams/current-block))
      (let [client (substreams/spawn-client)]
        (substreams/start-stream client)))))
