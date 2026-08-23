use color_eyre::eyre::{Result, WrapErr};
use serde::Deserialize;
use std::{collections::HashMap, path::Path};

use super::{
    ACCTID, BALAMT, CURDEF, DTASOF, DTPOSTED, FITID, MEMO, NAME, OFXHEADER, PAYEE, TRNAMT, TRNTYPE,
    VERSION, truncate_yyyymmdd,
};
use crate::hull::{Hull, Hulls};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct Document {
    bankmsgsrsv1: Option<BankMsgsRsV1>,
    creditcardmsgsrsv1: Option<CreditCardMsgsRsV1>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct BankMsgsRsV1 {
    stmttrnrs: Vec<StmtTrnRs>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct CreditCardMsgsRsV1 {
    ccstmttrnrs: Vec<CcStmtTrnRs>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct StmtTrnRs {
    stmtrs: Option<StmtRs>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct CcStmtTrnRs {
    ccstmtrs: Option<CcStmtRs>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct StmtRs {
    curdef: String,
    bankacctfrom: Option<BankAcctFrom>,
    banktranlist: Option<BankTranList>,
    ledgerbal: LedgerBal,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct CcStmtRs {
    curdef: String,
    ccacctfrom: Option<CcAcctFrom>,
    banktranlist: Option<BankTranList>,
    ledgerbal: LedgerBal,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct BankAcctFrom {
    acctid: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct CcAcctFrom {
    acctid: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct BankTranList {
    #[serde(rename = "STMTTRN")]
    stmttrns: Vec<StmtTrn>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct StmtTrn {
    trntype: String,
    dtposted: String,
    trnamt: String,
    fitid: String,
    name: Option<String>,
    payee: Option<Payee>,
    memo: Option<String>,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct Payee {
    name: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "UPPERCASE")]
struct LedgerBal {
    balamt: String,
    dtasof: String,
}

impl From<&StmtTrn> for HashMap<String, String> {
    fn from(value: &StmtTrn) -> Self {
        [
            (TRNTYPE, value.trntype.clone()),
            (DTPOSTED, truncate_yyyymmdd(value.dtposted.clone())),
            (TRNAMT, value.trnamt.clone()),
            (FITID, value.fitid.clone()),
        ]
        .into_iter()
        .chain(value.name.iter().map(|name| (NAME, name.clone())))
        .chain(value.payee.iter().map(|payee| (PAYEE, payee.name.clone())))
        .chain(value.memo.iter().map(|memo| (MEMO, memo.clone())))
        .map(|(k, v)| (k.to_string(), v))
        .collect::<HashMap<_, _>>()
    }
}

pub(crate) fn parse(
    path: &Path,
    ofx2_content: &str,
    ofxheader: &str,
    version: &str,
) -> Result<Hulls> {
    let doc = quick_xml::de::from_str::<'_, Document>(ofx2_content)
        .wrap_err_with(|| format!("Failed to decode OFX2 XML in {}", path.to_string_lossy()))?;

    let hulls = doc
        .bankmsgsrsv1
        .iter()
        .flat_map(|bankmsgsrsv1| {
            bankmsgsrsv1.stmttrnrs.iter().flat_map(|stmttrnrs| {
                stmttrnrs.stmtrs.iter().map(|stmtrs| {
                    (
                        &stmtrs.curdef,
                        stmtrs
                            .bankacctfrom
                            .as_ref()
                            .map(|bankacctfrom| &bankacctfrom.acctid),
                        stmtrs.banktranlist.as_ref(),
                        &stmtrs.ledgerbal,
                    )
                })
            })
        })
        .chain(doc.creditcardmsgsrsv1.iter().flat_map(|ccstmttrnrs| {
            ccstmttrnrs.ccstmttrnrs.iter().flat_map(|ccstmttrnrs| {
                ccstmttrnrs.ccstmtrs.iter().map(|ccstmtrs| {
                    (
                        &ccstmtrs.curdef,
                        ccstmtrs
                            .ccacctfrom
                            .as_ref()
                            .map(|ccacctfrom| &ccacctfrom.acctid),
                        ccstmtrs.banktranlist.as_ref(),
                        &ccstmtrs.ledgerbal,
                    )
                })
            })
        }))
        .map(
            |(curdef, acctid, banktranlist, ledgerbal): (
                &String,
                Option<&String>,
                Option<&BankTranList>,
                &LedgerBal,
            )| Hull {
                hdr: [
                    (OFXHEADER, ofxheader.to_string()),
                    (VERSION, version.to_string()),
                    (CURDEF, curdef.clone()),
                    (BALAMT, ledgerbal.balamt.clone()),
                    (DTASOF, truncate_yyyymmdd(ledgerbal.dtasof.clone())),
                ]
                .into_iter()
                // conditionally include acctid if exists
                .chain(acctid.map(|acctid| (ACCTID, acctid.clone())))
                .map(|(k, v)| (k.to_string(), v))
                .collect::<HashMap<_, _>>(),
                txns: banktranlist
                    .iter()
                    .flat_map(|banktranlist| {
                        banktranlist
                            .stmttrns
                            .iter()
                            .map(Into::<HashMap<_, _>>::into)
                    })
                    .collect::<Vec<_>>(),
            },
        )
        .collect::<Vec<_>>();

    Ok(Hulls(hulls))
}
