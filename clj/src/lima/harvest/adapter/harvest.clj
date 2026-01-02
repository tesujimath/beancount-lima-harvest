(ns lima.harvest.adapter.harvest
  (:require [cheshire.core :as cheshire]
            [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [lima.harvest.core.glob :as glob]
            [clojure.string :as str]
            [java-time.api :as jt]
            [lima.harvest.core.infer :as infer]
            [failjure.core :as f]))

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
  (println classified)
  (let [{:keys [ingester path]} classified
        cmd (substitute :path path ingester)
        dummy (println path ingester cmd)
        ingested (apply shell/sh cmd)]
    (if (= (:exit ingested) 0)
      (-> (:out ingested)
          (cheshire/parse-string true)
          (assoc :path path)
          (update :hdr #(merge % (:hdr classified))))
      (f/fail "%s failed: %s" (str/join " " cmd) (:err ingested)))))

(defn get-realizer
  "Find the first realizer whose selector matches the ingested header"
  [config ingested]
  (if-let [realizers (:realizers config)]
    (let [hdr (:hdr ingested)]
      (or (some (fn [[m v]] (and (= m (select-keys hdr (keys m))) v)) realizers)
          (f/fail "failed to find realizer for %s with hdr %s in %s"
                  (:path ingested)
                  (str hdr)
                  (:path config))))
    (f/fail "no realizers specified in %s" (:path config))))

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
        ;; TODO validate this ahead of time so we can't fail here
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
  "Transducer to realize transactions"
  [config digest realizer hdr]
  (map #(->> %
             (realize-txn (:txn realizer) (:txn-fn realizer) hdr)
             (infer-acc digest))))

(defn dedupe-transactions
  "Transducer to dedupe with respect to txnids"
  [txnids]
  (filter #(not (if-let [txnid (:txnid %)] (contains? txnids txnid)))))

(defn infer-secondary-accounts
  "Transducer to infer secondary accounrs from payees and narrations"
  [payees narrations]
  (map (infer/secondary-accounts payees narrations)))

(defn harvest-one
  "Harvest a single file as far as realizing"
  [config digest import-path]
  (f/attempt-all [classified (classify config digest import-path)
                  ingested (ingest classified)
                  realizer (get-realizer config ingested)
                  txns (into []
                             (comp
                               (realize config digest realizer (:hdr ingested))
                               (dedupe-transactions (:txnids digest))
                               (infer-secondary-accounts (:payees digest)
                                                         (:narrations digest)))
                             (:txns ingested))]
    (assoc ingested :txns txns)))

(defn harvest-all
  "Harvest several files"
  [config digest import-paths]
  (let [realizeds (mapv #(harvest-one config digest %) import-paths)]
    realizeds))
