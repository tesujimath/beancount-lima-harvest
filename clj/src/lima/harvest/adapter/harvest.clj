(ns lima.harvest.adapter.harvest
  (:require [cheshire.core :as cheshire]
            [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [lima.harvest.core.glob :as glob]
            [clojure.string :as str]
            [java-time.api :as jt]
            [lima.harvest.core.infer :as infer]
            [lima.harvest.core.realize :as realize]
            [failjure.core :as f]))


(defn classify
  "Classify an import.

  And augment the header according to hdr-fn, if any."
  [config digest import-path]
  (if-let [classifiers (:classifiers config)]
    (f/attempt-all [classified
                      (or (some (fn [c]
                                  (if-let [path-glob (:path-glob c)]
                                    (and (glob/match? path-glob import-path)
                                         (assoc c :path import-path))
                                    nil))
                                classifiers)
                          (f/fail
                            "failed to classify %s matching path-globs in %s"
                            import-path
                            (:path config)))
                    hdr-fn-sym (:hdr-fn classified)
                    hdr-fn (and hdr-fn-sym (resolve hdr-fn-sym))]
      (if hdr-fn (hdr-fn digest classified) classified))
    (f/fail "no classifiers specified in %s" (:path config))))

(defn substitute
  "Substitute k for v among items"
  [k v items]
  (mapv #(if (= % k) v %) items))

(defn ingest
  "Ingest an import file once it has been classified"
  [classified]
  (let [{:keys [ingester path]} classified
        cmd (substitute :path path ingester)
        ingested (apply shell/sh cmd)]
    (if (= (:exit ingested) 0)
      (-> (:out ingested)
          (cheshire/parse-string true)
          (assoc :path path)
          (update :hdr #(merge % (:hdr classified))))
      (f/fail "%s failed: %s" (str/join " " cmd) (:err ingested)))))
(defn dedupe-xf
  "Transducer to dedupe with respect to txnids"
  [txnids]
  (filter #(not (if-let [txnid (:txnid %)] (contains? txnids txnid)))))

(defn infer-secondary-accounts-xf
  "Transducer to infer secondary accounrs from payees and narrations"
  [payees narrations]
  (map (infer/secondary-accounts payees narrations)))

(defn harvest-one-txns
  "Harvest a file into txns eduction"
  [config digest import-path]
  (f/attempt-all [classified (classify config digest import-path)
                  ingested (ingest classified)
                  realizer (realize/get-realizer config ingested)]
    (eduction (comp (realize/xf config digest realizer (:hdr ingested))
                    (dedupe-xf (:txnids digest))
                    (infer-secondary-accounts-xf (:payees digest)
                                                 (:narrations digest)))
              (:txns ingested))))

(defn harvest-step
  "A single reducer step for wrapping the eduction and propagating failure"
  [config digest]
  (fn [fallible-result import-path]
    (f/attempt-all [txns (harvest-one-txns config digest import-path)
                    result fallible-result]
      (into result txns))))

(defn harvest-all
  "Harvest several files"
  [config digest import-paths]
  (reduce (harvest-step config digest) [] import-paths))
