(ns lima.core.import.ofx1
  (:require [java-time.api :as jt]))

(defn extract-balance
  "extract balance from header if we can find the keys we need, otherwise nil"
  [accounts-by-id source]
  (let [hdr (:header source)
        {:keys [curdef balamt dtasof]} hdr
        acctid (or (:acctid hdr) "unknown-acctid")
        account (or (get accounts-by-id acctid) "Assets:Unknown")]
    (and balamt
         dtasof
         ;; Beancount balance date is as of midnight at the beginning of
         ;; the day, but we have the end of the day, so add 1 day
         (let [date (jt/plus (jt/local-date dtasof (jt/formatter "yyyyMMdd"))
                             (jt/days 1))
               units (parse-decimal-cents balamt)]
           {:date date, :account account, :units units, :cur curdef}))))
