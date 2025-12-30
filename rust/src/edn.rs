use crate::digest::Digest;
use color_eyre::eyre::Result;
use std::fmt::{self, Display, Formatter, Write};
use std::iter::{once, repeat};
use strum_macros::{EnumIter, EnumString, IntoStaticStr};

impl Digest {
    pub(crate) fn write<W>(&self, out_w: W) -> Result<()>
    where
        W: std::io::Write + Copy,
    {
        use std::io::{BufWriter, Write};

        let mut buffered_out_w = BufWriter::new(out_w);
        writeln!(buffered_out_w, "{}\n", Edn(self))?;

        Ok(())
    }
}

// TODO improve this, it's a bit ugly
struct Edn<T>(T)
where
    T: FmtEdn + Clone;

impl<T> Display for Edn<T>
where
    T: FmtEdn + Clone,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        (self.0.clone()).fmt_edn(f)
    }
}

trait FmtEdn {
    fn fmt_edn(self, f: &mut Formatter<'_>) -> fmt::Result;
}

impl FmtEdn for &Digest {
    fn fmt_edn(self, f: &mut Formatter<'_>) -> fmt::Result {
        use Separator::*;

        map_begin(f)?;

        (
            Keyword::Accids,
            EdnMap(self.accids.iter().map(|(k, v)| (k.as_str(), v.as_str()))),
            Flush,
        )
            .fmt_edn(f)?;
        (
            Keyword::Txnids,
            EdnSet(self.txnids.iter().map(|x| x.as_str())),
            Spaced,
        )
            .fmt_edn(f)?;
        (
            Keyword::Payees,
            EdnMap(
                self.payees
                    .iter()
                    .map(|(x, m)| (x.as_str(), EdnMap(m.iter().map(|(k, v)| (k.as_str(), *v))))),
            ),
            Spaced,
        )
            .fmt_edn(f)?;
        (
            Keyword::Narrations,
            EdnMap(
                self.narrations
                    .iter()
                    .map(|(x, m)| (x.as_str(), EdnMap(m.iter().map(|(k, v)| (k.as_str(), *v))))),
            ),
            Spaced,
        )
            .fmt_edn(f)?;
        map_end(f)
    }
}

impl FmtEdn for usize {
    fn fmt_edn(self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{self}")
    }
}

impl FmtEdn for &str {
    fn fmt_edn(self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_char('"')?;
        for c in self.chars() {
            match c {
                '\t' => f.write_str(r#"\t"#)?,
                '\r' => f.write_str(r#"\r"#)?,
                '\n' => f.write_str(r#"\\n"#)?,
                '\\' => f.write_str(r#"\\"#)?,
                '\"' => f.write_str(r#"\"\""#)?,
                c => f.write_char(c)?,
            }
        }
        f.write_char('"')
    }
}

// ubiquitous keywords, e.g. currency, are abbreviated
#[derive(EnumString, EnumIter, IntoStaticStr, Clone, Debug)]
#[strum(serialize_all = "kebab-case")]
enum Keyword {
    Accids,
    Narrations,
    Payees,
    Txnids,
}

impl FmtEdn for Keyword {
    fn fmt_edn(self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, ":{}", Into::<&str>::into(self))
    }
}

#[derive(PartialEq, Eq, Clone, Debug)]
enum Separator {
    Flush,
    Spaced,
}

struct EdnSet<I, T>(I)
where
    I: Iterator<Item = T>;

impl<I, T> FmtEdn for EdnSet<I, T>
where
    I: Iterator<Item = T>,
    T: FmtEdn,
{
    fn fmt_edn(self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(SET_BEGIN)?;

        for (item, sep) in self.0.zip(separators()) {
            if sep == Separator::Spaced {
                f.write_str(SPACE)?;
            }
            item.fmt_edn(f)?;
        }
        f.write_str(SET_END)
    }
}

// a homgeneous map
struct EdnMap<I, K, V>(I)
where
    I: Iterator<Item = (K, V)>;

impl<I, K, V> FmtEdn for EdnMap<I, K, V>
where
    I: Iterator<Item = (K, V)>,
    K: FmtEdn,
    V: FmtEdn,
{
    fn fmt_edn(self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(MAP_BEGIN)?;

        for ((k, v), sep) in self.0.zip(separators()) {
            (k, v, sep).fmt_edn(f)?;
        }
        f.write_str(MAP_END)
    }
}

// a map entry with optional separator prefix
//
// we can't implement map formatting like we did with sets and vectors because of the heterogeny
impl<K, V> FmtEdn for (K, V, Separator)
where
    K: FmtEdn,
    V: FmtEdn,
{
    fn fmt_edn(self, f: &mut Formatter<'_>) -> fmt::Result {
        let (key, value, sep) = self;
        if sep == Separator::Spaced {
            f.write_str(COMMA_SPACE)?;
        }
        key.fmt_edn(f)?;
        f.write_str(SPACE)?;
        value.fmt_edn(f)
    }
}

fn map_begin(f: &mut Formatter<'_>) -> fmt::Result {
    f.write_str(MAP_BEGIN)
}

fn map_end(f: &mut Formatter<'_>) -> fmt::Result {
    f.write_str(MAP_END)
}

// an infinite iterator of separators, with the first one only being flush
fn separators() -> impl Iterator<Item = Separator> {
    use Separator::*;

    once(Flush).chain(repeat(Spaced))
}

const MAP_BEGIN: &str = "{";
const MAP_END: &str = "}";
const SET_BEGIN: &str = "#{";
const SET_END: &str = "}";

// separators
const COMMA_SPACE: &str = ", ";
const SPACE: &str = " ";
