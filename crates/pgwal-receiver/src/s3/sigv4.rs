//! AWS Signature Version 4 for S3 requests, on the client side. No I/O.

use hmac::{Hmac, Mac};
use sha2::{Digest, Sha256};

type HmacSha256 = Hmac<Sha256>;

const ALGORITHM: &str = "AWS4-HMAC-SHA256";

/// What [`uri_encode`] does with `/`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Slash {
    /// For query values and bucket names.
    Encode,
    /// For object keys in a path.
    Keep,
}

/// RFC 3986 encoding: everything but `A-Z a-z 0-9 - _ . ~` is `%XX`, and `/`
/// unless kept.
pub(crate) fn uri_encode(bytes: &[u8], slash: Slash) -> String {
    let mut out = String::with_capacity(bytes.len());
    for &b in bytes {
        let plain = b.is_ascii_alphanumeric()
            || matches!(b, b'-' | b'_' | b'.' | b'~')
            || (b == b'/' && slash == Slash::Keep);
        if plain {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

pub(crate) fn sha256_hex(data: &[u8]) -> String {
    hex::encode(Sha256::digest(data))
}

fn hmac(key: &[u8], data: &[u8]) -> [u8; 32] {
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC takes a key of any size");
    mac.update(data);
    mac.finalize().into_bytes().into()
}

/// A request to sign.
pub(crate) struct Request<'a> {
    pub method: &'a str,
    /// Already encoded.
    pub path: &'a str,
    /// `name=value` pairs, each part already encoded, in any order.
    pub query: &'a [(String, String)],
    /// Every header to sign, `host` included, with lowercase names.
    pub headers: &'a [(String, String)],
    pub payload_hash: &'a str,
}

/// The `Authorization` header value for `request`.
pub(crate) fn sign(
    request: &Request<'_>,
    access_key: &str,
    secret: &str,
    timestamp: &str,
    region: &str,
) -> String {
    let date = &timestamp[..8];
    let mut headers: Vec<(String, String)> = request
        .headers
        .iter()
        .map(|(n, v)| {
            (
                n.clone(),
                v.split_whitespace().collect::<Vec<_>>().join(" "),
            )
        })
        .collect();
    headers.sort();
    let signed = headers
        .iter()
        .map(|(n, _)| n.as_str())
        .collect::<Vec<_>>()
        .join(";");
    let canonical_headers: String = headers.iter().map(|(n, v)| format!("{n}:{v}\n")).collect();
    let mut query = request.query.to_vec();
    query.sort();
    let query = query
        .iter()
        .map(|(n, v)| format!("{n}={v}"))
        .collect::<Vec<_>>()
        .join("&");
    let canonical = format!(
        "{}\n{}\n{query}\n{canonical_headers}\n{signed}\n{}",
        request.method, request.path, request.payload_hash
    );

    let scope = format!("{date}/{region}/s3/aws4_request");
    let to_sign = format!(
        "{ALGORITHM}\n{timestamp}\n{scope}\n{}",
        sha256_hex(canonical.as_bytes())
    );
    let key = {
        let secret = zeroize::Zeroizing::new(format!("AWS4{secret}"));
        let date_key = hmac(secret.as_bytes(), date.as_bytes());
        let region_key = hmac(&date_key, region.as_bytes());
        let service_key = hmac(&region_key, b"s3");
        hmac(&service_key, b"aws4_request")
    };
    let signature = hex::encode(hmac(&key, to_sign.as_bytes()));
    format!(
        "{ALGORITHM} Credential={access_key}/{scope}, SignedHeaders={signed}, Signature={signature}"
    )
}

/// `20261005T120000Z`, the `x-amz-date` form of a unix time.
pub(crate) fn amz_date(unix_secs: u64) -> String {
    // Civil date of a day count (Howard Hinnant's algorithm).
    let z = (unix_secs / 86_400) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = yoe + era * 400 + i64::from(month <= 2);
    let secs = unix_secs % 86_400;
    format!(
        "{year:04}{month:02}{day:02}T{:02}{:02}{:02}Z",
        secs / 3600,
        secs % 3600 / 60,
        secs % 60
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    const EMPTY_SHA256: &str = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

    fn headers(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(n, v)| (n.to_string(), v.to_string()))
            .collect()
    }

    /// AWS documentation, "Signature Calculations for the Authorization
    /// Header", example GET Object.
    #[test]
    fn signs_the_aws_get_object_example() {
        let headers = headers(&[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("range", "bytes=0-9"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", "20130524T000000Z"),
        ]);
        let request = Request {
            method: "GET",
            path: "/test.txt",
            query: &[],
            headers: &headers,
            payload_hash: EMPTY_SHA256,
        };
        let header = sign(
            &request,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524T000000Z",
            "us-east-1",
        );
        assert_eq!(
            header,
            "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20130524/us-east-1/s3/aws4_request, \
             SignedHeaders=host;range;x-amz-content-sha256;x-amz-date, \
             Signature=f0e8bdb87c964420e857bd35b5d6ed310bd44f0170aba48dd91039c6036bdb41"
        );
    }

    /// AWS documentation, example GET Bucket (List Objects): a signed query.
    #[test]
    fn signs_the_aws_list_objects_example() {
        let headers = headers(&[
            ("host", "examplebucket.s3.amazonaws.com"),
            ("x-amz-content-sha256", EMPTY_SHA256),
            ("x-amz-date", "20130524T000000Z"),
        ]);
        let query = [
            ("prefix".to_string(), "J".to_string()),
            ("max-keys".to_string(), "2".to_string()),
        ];
        let request = Request {
            method: "GET",
            path: "/",
            query: &query,
            headers: &headers,
            payload_hash: EMPTY_SHA256,
        };
        let header = sign(
            &request,
            "AKIAIOSFODNN7EXAMPLE",
            "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "20130524T000000Z",
            "us-east-1",
        );
        assert!(
            header.ends_with(
                "Signature=34b48302e7b5fa45bde8084f4b7868a86f0a534bc59db6670ed5711ef69dc6f7"
            ),
            "{header}"
        );
    }

    #[test]
    fn encodes_keys_and_dates() {
        assert_eq!(uri_encode(b"7/0000/a b", Slash::Keep), "7/0000/a%20b");
        assert_eq!(uri_encode(b"a/b", Slash::Encode), "a%2Fb");
        assert_eq!(amz_date(0), "19700101T000000Z");
        assert_eq!(amz_date(1_793_995_200), "20261106T200000Z");
    }
}
