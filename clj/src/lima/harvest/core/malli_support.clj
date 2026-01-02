(ns lima.harvest.core.malli-support
  (:require [failjure.core :as f]
            [malli.core :as m]
            [malli.error :as me]))

(defn validate-or-fail
  [spec x context]
  (if-let [err (m/explain spec x)]
    (f/fail "%s validation failed: %s" context (me/humanize err))
    x))
