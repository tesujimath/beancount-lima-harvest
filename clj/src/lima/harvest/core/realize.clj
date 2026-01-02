(ns lima.harvest.core.realize
  (:require [clojure.string :as str]
            [java-time.api :as jt]
            [failjure.core :as f]))

(defn get-realizer
  "Find the first realizer whose selector matches the ingested header"
  {:malli/schema [:-> any? any? Realizer]}
  [config ingested]
  (if-let [realizers (:realizers config)]
    (let [hdr (:hdr ingested)
          r (some (fn [[m v]] (and (= m (select-keys hdr (keys m))) v))
                  realizers)]
      (or r
          (f/fail "failed to find realizer for %s with hdr %s in %s"
                  (:path ingested)
                  (str hdr)
                  (:path config))))
    (f/fail "no realizers specified in %s" (:path config))))

(defn realize-field
  [hdr txn r]
  (cond (string? r) r
        (map? r) (let [{:keys [:fmt :key :src :type]} r]
                   (let [src (case src
                               :hdr hdr
                               :txn txn)
                         v-raw (get src key)
                         fmt (or fmt "yyyy-MM-dd")]
                     (case type
                       :date (jt/local-date fmt v-raw)
                       :decimal (BigDecimal. v-raw)
                       nil v-raw)))
        (vector? r) (str/join "" (map #(realize-field hdr txn %) r))
        ;; TODO validate this ahead of time so we can't fail here
        :else (throw (Exception. (str "bad realizer val " r)))))

(defn realize-txn
  "Realize the transaction, and if f-sym is defined, resolve and apply that after the event."
  [realizer f-sym hdr txn]
  (let [m (into {}
                (map (fn [[k v]] [k (realize-field hdr txn (get realizer k))])
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

(defn xf
  "Transducer to realize transactions"
  [config digest realizer hdr]
  (map #(->> %
             (realize-txn (:txn realizer) (:txn-fn realizer) hdr)
             (infer-acc digest))))
