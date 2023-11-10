(ns geo-sink.core
  (:gen-class)
  (:require [clojure.spec.alpha :as s]
            [dotenv :refer [env]]
            [geo-sink.cache :refer [cached-actions cached-roles-granted
                                    write-cursor-cache-file
                                    cached-roles-revoked]]
            [geo-sink.constants :refer [cache-action-directory
                                        cache-cursor-file cache-entry-directory
                                        cache-granted-directory cache-revoked-directory geo-genesis-start-block]]
            [geo-sink.db-helpers :refer [reset-geo-db]]
            [geo-sink.populate :refer [actions->db role-granted->db
                                       role-revoked->db]]
            [geo-sink.substreams :as substreams]))

(s/check-asserts true)


(defn -main
  "Core entrypoint for the geo-sink application"
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
    (doseq [path [cache-entry-directory cache-granted-directory cache-revoked-directory cache-action-directory]]
      (when-not (.exists (java.io.File. path))
        (.mkdirs (java.io.File. path))))
    
    ;; Create required cache file
    (when-not (.exists (java.io.File. cache-cursor-file))
      (write-cursor-cache-file geo-genesis-start-block "")
    )

    ;; --reset-db flag to clear and bootstrap the database with some fundamental entities
    (when reset-db
      (println "Resetting database. Exiting...")
      (reset-geo-db)
      (System/exit 0))

    ;; --reset-cursor flag to reset the cursor to the default start block
    (when reset-cursor
      (println "Resetting cursor to geo genesis start block")
      (swap! substreams/current-block (fn [_] (str geo-genesis-start-block)))
      (swap! substreams/cursor (fn [_] "")))

    ;; --from-cache flag to populate the database from the cache
    (when from-cache
      (println "Syncing actions and roles from cache...")
      (doseq [actions cached-actions]
        (actions->db actions))
      (doseq [roles cached-roles-granted]
        (doseq [role roles]
          (role-granted->db role)))
      (doseq [roles cached-roles-revoked]
        (doseq [role roles]
          (role-revoked->db role)))
      (println "Done syncing from cache. Exiting...") 
      )


    ;; Start streaming the substreams client and populating the cache when --from-cache is not set (TODO: Explore auto-streaming after cache is populated)
    (while (not from-cache)
      (println "from-stream block #" (str @substreams/current-block))
      (let [client (substreams/spawn-client)]
        (substreams/start-stream client)))))
