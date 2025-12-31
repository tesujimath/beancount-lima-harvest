use color_eyre::eyre::{eyre, Result};
use serde::Deserialize;
use std::{collections::HashMap, path::Path};

use crate::hull::Hull;

#[derive(Deserialize, Debug)]
struct Document {
    bankmsgsrsv1: Option<BankMsgsRsV1>,
    creditcardmsgsrsv1: Option<CreditCardMsgsRsV1>,
}

#[derive(Deserialize, Debug)]
struct BankMsgsRsV1 {
    stmttrnrs: StmtTrnRs,
}

#[derive(Deserialize, Debug)]
struct CreditCardMsgsRsV1 {
    ccstmttrnrs: CcStmtTrnRs,
}

#[derive(Deserialize, Debug)]
struct StmtTrnRs {
    stmtrs: StmtRs,
}

#[derive(Deserialize, Debug)]
struct CcStmtTrnRs {
    ccstmtrs: CcStmtRs,
}

#[derive(Deserialize, Debug)]
struct StmtRs {
    curdef: String,
    bankacctfrom: BankAcctFrom,
    banktranlist: BankTranList,
    ledgerbal: LedgerBal,
}

#[derive(Deserialize, Debug)]
struct CcStmtRs {
    curdef: String,
    ccacctfrom: CcAcctFrom,
    banktranlist: BankTranList,
    ledgerbal: LedgerBal,
}

#[derive(Deserialize, Debug)]
struct BankAcctFrom {
    acctid: String,
}

#[derive(Deserialize, Debug)]
struct CcAcctFrom {
    acctid: String,
}

#[derive(Deserialize, Debug)]
struct BankTranList {
    #[serde(rename = "stmttrn")]
    stmttrns: Vec<StmtTrn>,
}

#[derive(Deserialize, Debug)]
struct StmtTrn {
    trntype: String,
    dtposted: String,
    trnamt: String,
    fitid: String,
    name: String,
    memo: String,
}

#[derive(Deserialize, Debug)]
struct LedgerBal {
    balamt: String,
    dtasof: String,
}

impl From<StmtTrn> for HashMap<String, String> {
    fn from(value: StmtTrn) -> Self {
        [
            ("trntype", value.trntype),
            ("dtposted", value.dtposted),
            ("trnamt", value.trnamt),
            ("fitid", value.fitid),
            ("name", value.name),
            ("memo", value.memo),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect::<HashMap<_, _>>()
    }
}

pub(crate) fn parse(path: &Path, ofx_content: &str) -> Result<Hull> {
    let sgml = sgmlish::Parser::builder()
        .lowercase_names()
        .expand_entities(|entity| match entity {
            "lt" => Some("<"),
            "gt" => Some(">"),
            "amp" => Some("&"),
            "nbsp" => Some(" "),
            _ => None,
        })
        .parse(ofx_content)?;
    let sgml = sgmlish::transforms::normalize_end_tags(sgml)?;
    let doc = sgmlish::from_fragment::<Document>(sgml)?;

    match doc {
        Document {
            bankmsgsrsv1:
                Some(BankMsgsRsV1 {
                    stmttrnrs:
                        StmtTrnRs {
                            stmtrs:
                                StmtRs {
                                    curdef,
                                    bankacctfrom: BankAcctFrom { acctid },
                                    banktranlist: BankTranList { stmttrns },
                                    ledgerbal: LedgerBal { balamt, dtasof },
                                },
                        },
                }),
            creditcardmsgsrsv1: None,
        } => Ok((curdef, acctid, balamt, dtasof, stmttrns)),

        Document {
            bankmsgsrsv1: None,
            creditcardmsgsrsv1:
                Some(CreditCardMsgsRsV1 {
                    ccstmttrnrs:
                        CcStmtTrnRs {
                            ccstmtrs:
                                CcStmtRs {
                                    curdef,
                                    ccacctfrom: CcAcctFrom { acctid },
                                    banktranlist: BankTranList { stmttrns },
                                    ledgerbal: LedgerBal { balamt, dtasof },
                                },
                        },
                }),
        } => Ok((curdef, acctid, balamt, dtasof, stmttrns)),

        _ => Err(eyre!("unsupported OFX1 document {:?}", path)),
    }
    .map(|(curdef, acctid, balamt, dtasof, stmttrns)| Hull {
        hdr: [
            ("dialect", "ofx1".to_string()),
            ("curdef", curdef),
            ("acctid", acctid),
            ("balamt", balamt),
            ("dtasof", dtasof),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect::<HashMap<_, _>>(),
        txns: stmttrns
            .into_iter()
            .map(Into::<HashMap<_, _>>::into)
            .collect::<Vec<_>>(),
    })
}
