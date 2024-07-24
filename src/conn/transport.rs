//! Transports for communicating with the Podman or Docker daemon

use crate::conn::{Error, Headers, Payload, Result};

use futures_util::{
    stream::{self, Stream},
    StreamExt, TryStreamExt,
};
use hyper::{
    body::Bytes,
    header, Method,
};
use reqwest::{Body, Client, Request, Response};

#[cfg(unix)]
use hyperlocal::UnixConnector;
#[cfg(unix)]
use hyperlocal::Uri as DomainUri;
use url::Url;

use std::{iter::IntoIterator, path::PathBuf};

/// Transports are types which define supported means of communication.
#[derive(Clone, Debug)]
pub enum Transport {
    /// A network tcp interface
    Tcp {
        client: reqwest::Client,
        host: Url,
    },
    /// TCP/TLS
    #[cfg(feature = "tls")]
    #[cfg_attr(docsrs, doc(cfg(feature = "tls")))]
    EncryptedTcp {
        client: reqwest::Client,
        host: Url,
    },
    /// A Unix domain socket
    #[cfg(unix)]
    Unix {
        client: reqwest::Client,
        path: PathBuf,
    },
}

impl Transport {

    pub fn get_client(&self) -> &Client {
        match &self {
            Transport::Tcp { client, host } => client,
            Transport::Unix { client, path } => client,
            #[cfg(feature = "tls")]
            Transport::EncryptedTcp { client, host } => client,
        }
    }
    pub fn remote_addr(&self) -> &str {
        match &self {
            Self::Tcp { ref host, .. } => host.as_ref(),
            #[cfg(feature = "tls")]
            Self::EncryptedTcp { ref host, .. } => host.as_ref(),
            #[cfg(unix)]
            Self::Unix { ref path, .. } => path.to_str().unwrap_or_default(),
        }
    }

    pub fn make_uri(&self, ep: &str) -> Result<hyper::Uri> {
        match self {
            Transport::Tcp { host, .. } => format!("{host}{ep}").parse().map_err(Error::InvalidUri),
            #[cfg(feature = "tls")]
            Transport::EncryptedTcp { host, .. } => {
                format!("{host}{ep}").parse().map_err(Error::InvalidUri)
            }
            #[cfg(unix)]
            Transport::Unix { path, .. } => Ok(DomainUri::new(path, ep).into()),
        }
    }

    /// Send the given request and return a Future of the response.
    pub async fn request(&self, req: Request) -> Result<Response> {
        log::trace!("sending request {} {}", req.method(), req.url());
        match self {
            Transport::Tcp { ref client, .. } => client.execute(req),
            #[cfg(feature = "tls")]
            Transport::EncryptedTcp { ref client, .. } => client.execute(req),
            #[cfg(unix)]
            Transport::Unix { ref client, .. } => client.execute(req),
        }
        .await
        .map_err(Error::from)
    }

    pub async fn request_string(&self, req: Request) -> Result<String> {
        let body = self.request(req).await?;
        body_to_string(body).await
    }
}

pub(crate) async fn body_to_string(body: Response) -> Result<String> {
    let bytes = body.bytes().await?;
    String::from_utf8(bytes.to_vec()).map_err(Error::from)
}

/// Builds an HTTP request.
pub(crate) fn build_request<B>(
    client: &reqwest::Client,
    method: Method,
    uri: hyper::Uri,
    body: Payload<B>,
    headers: Option<Headers>,
) -> Result<Request>
where
    B: Into<Body>,
{
    let req = client.request(method, uri.to_string());
    // let req = builder.method(method).uri(&uri);
    let mut req = req.header(header::HOST, "");

    if let Some(h) = headers {
        for (k, v) in h.into_iter() {
            req = req.header(k, v);
        }
    }

    // early return
    if body.is_none() {
        return Ok(req.build()?);
    }

    let mime = body.mime_type();
    if let Some(c) = mime {
        req = req.header(header::CONTENT_TYPE, &c.to_string());
    }

    // it's ok to unwrap, we check that the body is not none
    req.body(body.into_inner().unwrap().into())
        .build()
        .map_err(Error::from)
}

pub(crate) async fn get_response_string(response: Response) -> Result<String> {
    body_to_string(response).await
}

pub(crate) fn stream_response(response: Response) -> impl Stream<Item = Result<Bytes>> {
    stream_body(response)
}

pub(crate) fn stream_json_response(response: Response) -> impl Stream<Item = Result<Bytes>> {
    stream_json_body(response)
}

fn stream_body(body: Response) -> impl Stream<Item = Result<Bytes>> {
    body.bytes_stream().map_err(Error::from)
}

static JSON_WHITESPACE: &[u8] = b"\r\n";

fn stream_json_body(body: Response) -> impl Stream<Item = Result<Bytes>> {
    async fn unfold(mut body: Response) -> Option<(Result<Bytes>, Response)> {
        let mut chunk = Vec::new();

        loop {
            let chnk = body.chunk().await;
            match chnk {
                Ok(Some(chnk)) => {
                    chunk.extend(chnk.to_vec());
                    if chnk.ends_with(JSON_WHITESPACE) {
                        break;
                    }
                }
                Ok(None) => {
                    return None;
                }
                Err(e)=> {
                    return Some((Err(Error::from(e)), body));
                }
            }
        }
        // while let Some(chnk) = body.chunk().await {
        //     match chnk {
        //         Ok(chnk) => {
        //             chunk.extend(chnk.to_vec());
        //             if chnk.ends_with(JSON_WHITESPACE) {
        //                 break;
        //             }
        //         }
        //         Err(e) => {
        //             return Some((Err(Error::from(e)), body));
        //         }
        //     }
        // }

        // if chunk.is_empty() {
        //     return None;
        // }

        Some((Ok(Bytes::from(chunk)), body))
    }

    stream::unfold(body, unfold)
}
