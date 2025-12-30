(ns lima.harvest.adapter.beanfile
  (:require [clojure.edn :as edn]
            [clojure.java.shell :as shell]
            [java-time.api :as jt]))

(def readers {'time/date #(jt/local-date %)})

(defn read-edn-string
  "Read string as Lima PP EDN"
  [s]
  (edn/read-string {:readers readers} s))

(defn digest
  "Read EDN from lima-digest and return or throw"
  [beancount-path]
  (let [digested (shell/sh "lima-digest" beancount-path)]
    (if (= (digested :exit) 0)
      (read-edn-string (digested :out))
      (do (println "lima-digest error" (digested :err))
          (throw (Exception. "lima-digest failed"))))))
