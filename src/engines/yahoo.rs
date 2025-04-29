//! The `yahoo` module handles the scraping of results from the yahoo search engine
//! by querying the upstream yahoo search engine with user provided query and with a page

use std::collections::HashMap;

use reqwest::header::HeaderMap;

// use reqwest::{Client, Error};

use reqwest::Client;

use scraper::Html;

use crate::models::aggregation::SearchResult;

use crate::models::engine::{EngineError, SearchEngine};

use error_stack::{Report, Result, ResultExt};

use super::search_result_parser::SearchResultParser;

/// A new Yahoo engine type defined in-order to implement the `SearchEngine` trait which allows to
/// reduce code duplication as well as allows to create vector of different search engines easily.
pub struct Yahoo {
    /// The parser, used to interpret the search result.
    parser: SearchResultParser,
    // Used to retrieve the original link from redirect_url.
    // client: Client,
}

impl Yahoo {
    /// Creates the Yahoo parser.
    pub fn new() -> Result<Self, EngineError> {
        Ok(Self {
            parser: SearchResultParser::new(
                ".compNoResult",
                "div.algo",
                "h3.title a",
                "h3 a",
                ".compText",
            )?,
            // client: Client::new(),
        })
    }
    //TODO: Function not implemented yet
    //
    // Function to fetch the final destination URL after handling redirects
    // Yahoo search results provide a redirect link when scraping HTML. This function helps retrieve the final URL.
    // async fn get_final_url(&self, redirect_url: &str) -> Result<String, Error> {
    //     // Send a GET request and let it follow redirects
    //     let response = self.client.get(redirect_url).send().await?;
    //
    //     // Extract the final destination URL (after following redirects)
    //     let final_url = response.url().as_str().to_string();
    //
    //     Ok(final_url)
    // }
}
fn parse_yahoo_redirect_url(raw_url: &str) -> String {
    // Look for the /RU= marker
    if let Some(start_idx) = raw_url.find("/RU=") {
        let encoded_start = &raw_url[start_idx + 4..]; // skip "/RU="
        let end_markers = ["/RS", "/RK"];
        let end_idx = end_markers
            .iter()
            .filter_map(|marker| encoded_start.find(marker))
            .min()
            .unwrap_or(encoded_start.len());

        let encoded_url = &encoded_start[..end_idx];

        // Manual URL decode using url::form_urlencoded
        match percent_decode(encoded_url.as_bytes()) {
            Ok(decoded) => decoded,
            Err(_) => raw_url.to_string(), // fallback
        }
    } else {
        raw_url.to_string()
    }
}
/// Perform a percent-decoding using only the Rust standard library.

fn percent_decode(input: &[u8]) -> Result<String, error_stack::Report<std::string::FromUtf8Error>> {
    let mut output = Vec::with_capacity(input.len());
    let mut i = 0;

    while i < input.len() {
        match input[i] {
            b'%' if i + 2 < input.len() => {
                if let (Some(h), Some(l)) = (from_hex(input[i + 1]), from_hex(input[i + 2])) {
                    output.push(h * 16 + l);
                    i += 3;
                } else {
                    // Invalid percent-encoding, keep literal
                    output.push(input[i]);
                    i += 1;
                }
            }
            b => {
                output.push(b);
                i += 1;
            }
        }
    }

    // Wrap the FromUtf8Error into a Report if there is an error
    String::from_utf8(output).map_err(|e| Report::new(e))
}


/// Convert a single ASCII hex character to its value.
fn from_hex(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}




#[async_trait::async_trait]
impl SearchEngine for Yahoo {
    async fn results(
        &self,
        query: &str,
        page: u32,
        user_agent: &str,
        client: &Client,
        _safe_search: u8,
    ) -> Result<Vec<(String, SearchResult)>, EngineError> {
        // Page number can be missing or empty string and so appropriate handling is required
        // so that upstream server recieves valid page number.
        let url: String = if page == 0 {
            format!("https://search.yahoo.com/search/?p={}", query)
        } else {
            format!(
                "https://search.yahoo.com/search/?p={}&b={}",
                query,
                (page * 10) + 1
            )
        };

        // initializing HeaderMap and adding appropriate headers.
        let header_map = HeaderMap::try_from(&HashMap::from([
            ("User-Agent".to_string(), user_agent.to_string()),
            ("Referer".to_string(), "https://google.com/".to_string()),
            (
                "Content-Type".to_string(),
                "application/x-www-form-urlencoded".to_string(),
            ),
            ("Cookie".to_string(), "kl=wt-wt".to_string()),
        ]))
        .change_context(EngineError::UnexpectedError)?;

        let document: Html = Html::parse_document(
            &Yahoo::fetch_html_from_upstream(self, &url, header_map, client).await?,
        );

        if self.parser.parse_for_no_results(&document).next().is_some() {
            return Err(Report::new(EngineError::EmptyResultSet));
        }

        self.parser
            .parse_for_results(&document, |title, url, desc| {
                // Scrape the HTML to extract and clean the data.
                let cleaned_title = title
                    .attr("aria-label")
                    .unwrap_or("No Title Found")
                    .trim()
                    .to_owned();
                let raw_url = url
                    .value()
                    .attr("href")
                    .unwrap_or("No Link Found");
                
                let cleaned_url = parse_yahoo_redirect_url(raw_url);

                let cleaned_description = desc.inner_html().trim().to_owned();
                Some(SearchResult::new(
                    &cleaned_title,
                    &cleaned_url,
                    &cleaned_description,
                    &["yahoo"],
                ))
            })
    }
}
