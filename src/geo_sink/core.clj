(ns geo-sink.core
  (:gen-class)
  (:require [clojure.spec.alpha :as s]
            [dotenv :refer [env]]
            [geo-sink.cache :refer [cached-actions cached-roles-granted
                                    cached-roles-revoked read-cursor-cache-file
                                    write-cursor-cache-file]]
            [geo-sink.constants :refer [cache-action-directory
                                        cache-cursor-file cache-entry-directory
                                        cache-granted-directory cache-revoked-directory geo-genesis-start-block]]
            [geo-sink.db-helpers :refer [reset-geo-db update-db-cursor]]
            [geo-sink.populate :refer [actions->db role-granted->db
                                       role-revoked->db]]
            [geo-sink.substreams :as substreams]))
(s/check-asserts true)


(defn -main
  "Core entrypoint for the geo-sink application"
  [& args]

  (let [args (into #{} args)
        from-genesis (get args "--from-genesis")
        from-cache (get args "--from-cache")
        populate-cache (get args "--populate-cache")]

    ;; Check for required environment variables
    (let [env-vars ["SUBSTREAMS_API_TOKEN" "SUBSTREAMS_ENDPOINT" "PGDATABASE" "PGUSER" "PGPASSWORD" "PGPORT" "PGHOST"]]
      (doseq [env-var env-vars]
        (when (not (env env-var))
          (throw (Exception. (str "Environment variable " env-var " is not defined"))))))

    ;; Hande populate-cache logic
    ;; Using substreams/is-populate-cache to help with passing populate-cache to watcher fn
   (swap! substreams/is-populate-cache (fn [_] populate-cache)) 
    (when (populate-cache)
    ;; Create required cache directories
      (doseq [path [cache-entry-directory cache-granted-directory cache-revoked-directory cache-action-directory]]
        (when-not (.exists (java.io.File. path))
          (.mkdirs (java.io.File. path))))

    ;; Create required cache file
      (when-not (.exists (java.io.File. cache-cursor-file))
        (write-cursor-cache-file geo-genesis-start-block "")))

    ;; From genesis or cache flag to clear and bootstrap the database with some fundamental entities
    (when (or from-genesis from-cache)
      (println "Resetting database...")
      (swap! substreams/current-block (fn [_] (str geo-genesis-start-block)))
      (swap! substreams/cursor (fn [_] ""))
      (reset-geo-db)
      (println "Done resetting database."))


    ;; From cache flag to populate the database from the cache
    (when from-cache
      (let [cursor-cache (read-cursor-cache-file)]
        (println (str
                  "Syncing from cache:\n"
                  "Cursor Cache: " (:cursor cursor-cache) "\n"
                  "Block Number: " (:block-number cursor-cache) "\n"
                  "Actions: " (apply + (map count cached-actions)) "\n"
                  "Roles granted: " (apply + (map count cached-roles-granted)) "\n"
                  "Roles revoked: " (apply + (map count cached-roles-revoked)) "\n"))

        (println "Handling cached actions...")
        (doseq [actions cached-actions]
          (actions->db actions))
        (println "Done handling cached actions.")

        (println "Handling cached roles...")
        (doseq [roles cached-roles-granted]
          (doseq [role roles]
            (role-granted->db role)))
        (println "Done handling cached roles.")

        (println "Handling cached roles revoked...")
        (doseq [roles cached-roles-revoked]
          (doseq [role roles]
            (role-revoked->db role)))
        (println "Done handling cached roles revoked.")

        (update-db-cursor (:cursor cursor-cache) (:block-number cursor-cache))
        (println "Done syncing cache with database.")))


    ;; Start streaming the substreams client and populating the cache when populate-cache is true)
    (while true
      (println "Streaming block #" (str @substreams/current-block))
      (let [client (substreams/spawn-client)]
        (substreams/start-stream client populate-cache)))))
