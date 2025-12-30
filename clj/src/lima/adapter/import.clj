(ns lima.adapter.import
  (:require [cheshire.core :as cheshire]
            [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [lima.core.glob :as glob]
            [clojure.string :as str]))

;; TODO better default config
(def DEFAULT-CONFIG {:path "default config"})

(defn read-config
  "Read import config from EDN file"
  [config-path]
  (let [config (slurp config-path)]
    (assoc (edn/read-string config) :path config-path)))

(defn classify
  "Classify an import"
  [config import-path]
  (if-let [classifiers (:classifiers config)]
    (or (some (fn [c]
                (if-let [path-glob (:path-glob c)]
                  (and (glob/match? path-glob import-path)
                       (assoc c :path import-path))
                  nil))
              classifiers)
        (throw (Exception. (str "failed to classify " import-path
                                " matching path-globs in " (:path config)))))
    (throw (Exception. (str "no classifiers specified in " (:path config))))))

(defn ingest
  "Ingest an import file once it has been classified"
  [classified]
  (let [{:keys [ingester path]} classified
        ingest-cmd (mapv #(if (= % :path) path %) ingester)
        ingested (apply shell/sh ingest-cmd)]
    (if (= (:exit ingested) 0)
      (let [ingested0 (cheshire/parse-string (:out ingested) true)
            ingested1 (assoc ingested0 :path path)
            ingested2 (update ingested1 :hdr #(merge % (:hdr classified)))]
        ingested2)
      (throw (Exception.
               (str (str/join " " ingest-cmd) " failed: " (:err ingested)))))))
