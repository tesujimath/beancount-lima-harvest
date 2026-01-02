(ns lima.harvest.core.config
  (:require [malli.core :as m]))



;; TODO this is a WIP

(def Field
  "Malli spec for a field"
  [:map [:key keyword?] [:src [:alt :hdr :txn]]
   [:type {:optional true} [:alt :date :decimal]
    [:fmt {:optional true} string?]]])

(def Realizer
  "Malli spec for a realizer."
  [:map [:bal {:optional true} [:map-of keyword? [:alt Field [:vec Field]]]]
   [:txn [:map-of keyword? [:alt Field [:vec Field]]]]
   [:txn-fn {:optional true} symbol?]])


(def Classifier
  [:map [:ingester [:vector [:or :string [:enum :path]]]] [:path-glob string?]
   [:hdr {:optional true} [:map-of keyword? string?]]
   [:hdr-fn {:optional true} symbol?]])

(def Config [:map [:classifiers [:vector Classifier]] [:path string?]])

;; TODO better default config
(def DEFAULT-CONFIG {:path "default config"})
