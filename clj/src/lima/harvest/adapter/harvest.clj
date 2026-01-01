(ns lima.harvest.adapter.harvest
  (:require [cheshire.core :as cheshire]
            [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [lima.harvest.core.glob :as glob]
            [clojure.string :as str]
            [java-time.api :as jt]
            [lima.harvest.core.infer :as infer]))

;; TODO better default config
(def DEFAULT-CONFIG {:path "default config"})

(defn read-config
  "Read harvest config from EDN file"
  [config-path]
  (let [config (slurp config-path)]
    (assoc (edn/read-string config) :path config-path)))

(defn classify
  "Classify an import.

  And augment the header according to hdr-fn, if any."
  [config digest import-path]
  (if-let [classifiers (:classifiers config)]
    (let [classified (or (some (fn [c]
                                 (if-let [path-glob (:path-glob c)]
                                   (and (glob/match? path-glob import-path)
                                        (assoc c :path import-path))
                                   nil))
                               classifiers)
                         (throw (Exception. (str "failed to classify "
                                                   import-path
                                                 " matching path-globs in "
                                                   (:path config)))))]
      (let [hdr-fn-sym (:hdr-fn classified)
            hdr-fn (and hdr-fn-sym (resolve hdr-fn-sym))]
        (if hdr-fn (hdr-fn digest classified) classified)))
    (throw (Exception. (str "no classifiers specified in " (:path config))))))

(defn ingest
  "Ingest an import file once it has been classified"
  [classified]
  (let [{:keys [ingester path]} classified
        ingest-cmd (mapv #(if (= % :path) path %) ingester)
        ingested (apply shell/sh ingest-cmd)]
    (if (= (:exit ingested) 0)
      (-> (:out ingested)
          (cheshire/parse-string true)
          (assoc :path path)
          (update :hdr #(merge % (:hdr classified))))
      (throw (Exception.
               (str (str/join " " ingest-cmd) " failed: " (:err ingested)))))))

(defn get-first-matching-realizer
  "Find the first realizer whose selector matches the ingested header"
  [config ingested]
  (if-let [realizers (:realizers config)]
    (let [hdr (:hdr ingested)]
      (or (some (fn [[m v]] (and (= m (select-keys hdr (keys m))) v)) realizers)
          (throw (Exception. (str "failed to find realizer for " (:path
                                                                   ingested)
                                  " with hdr " hdr
                                  " in " (:path config))))))
    (throw (Exception. (str "no realizers specified in " (:path config))))))

(defn realize-field
  [r hdr txn]
  (cond (string? r) r
        (vector? r) (let [[r-k & r-args] r]
                      (if (= :concat r-k)
                        (str/join "" (map #(realize-field % hdr txn) r-args))
                        (let [src (case r-k
                                    :hdr hdr
                                    :txn txn)
                              [k type fmt] r-args
                              v-raw (get src k)]
                          (case type
                            :date (jt/local-date fmt v-raw)
                            :decimal (BigDecimal. v-raw)
                            nil v-raw))))
        :else (throw (Exception. (str "bad realizer val " r)))))

(defn realize-txn
  "Realize the transaction, and if f-sym is defined, resolve and apply that after the event."
  [realizer f-sym hdr txn]
  (let [m (into {}
                (map (fn [[k v]] [k (realize-field (get realizer k) hdr txn)])
                  realizer))]
    (if f-sym (let [f (resolve f-sym)] (f m)) m)))

(defn infer-acc
  "Lookup the accid if any in the digest and infer the account"
  [digest txn]
  (if-let [accid (:accid txn)]
    (if-let [acc (get (digest :accids) accid)]
      (assoc txn :acc acc)
      txn)
    txn))

(defn realize
  "Realize an ingested file by matching and accid lookup"
  [config digest ingested]
  (let [realizer (get-first-matching-realizer config ingested)]
    {:txns (mapv #(->> %
                       (realize-txn (:txn realizer)
                                    (:txn-fn realizer)
                                    (:hdr ingested))
                       (infer-acc digest))
             (:txns ingested))}))

(defn dedupe
  "Dedupe with respect to txnids in the digest"
  [txnids realized]
  (assoc realized
    :txns (filterv #(not (if-let [txnid (:txnid %)] (contains? txnids txnid)))
            (:txns realized))))

(defn infer-secondary-accounts
  [payees narrations m]
  (assoc m :txns (mapv (infer/secondary-accounts payees narrations) (:txns m))))

(defn harvest-one
  "Harvest a single file as far as realizing"
  [config digest import-path]
  (->> import-path
       (classify config digest)
       (ingest)
       (realize config digest)
       (dedupe (:txnids digest))
       (infer-secondary-accounts (:payees digest) (:narrations digest))))

(defn harvest-all
  "Harvest several files"
  [config digest import-paths]
  (let [realizeds (mapv #(harvest-one config digest %) import-paths)]
    realizeds))
