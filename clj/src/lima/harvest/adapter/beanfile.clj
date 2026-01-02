(ns lima.harvest.adapter.beanfile
  (:require [cheshire.core :as cheshire]
            [clojure.java.shell :as shell]
            [failjure.core :as f]
            [java-time.api :as jt]
            [clojure.string :as str]))

(def readers {'time/date #(jt/local-date %)})

(def EMPTY-DIGEST {:accids {}, :txnids #{}, :payees {}, :narrations {}})

(defn digest
  "Read JSON from lima-digest and return ok or error map."
  [beancount-path]
  (let [cmd ["lima-digest" beancount-path]
        digested (apply shell/sh cmd)]
    (if (= (digested :exit) 0)
      (let [d0 (cheshire/parse-string (digested :out))
            ;; make keys at top-level into keywords, leaving others as
            ;; strings, because we have maps of payees, accids, etc.
            d1 (into {} (map (fn [[k v]] [(keyword k) v]) d0))]
        ;; JSON represents the set of txnids as a list, so fix that:
        (assoc d1 :txnids (set (:txnids d1))))
      (f/fail "%s failed: %s" (str/join " " cmd) (digested :err)))))
