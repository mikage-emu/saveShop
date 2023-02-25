use std::fs;
use std::fs::File;
use std::io::Write;
use std::collections::HashMap;
use std::collections::HashSet;

use std::fmt;

use std::thread;
use std::time;

use clap::Parser;

use serde::Deserialize;
use serde::de::DeserializeOwned;

use std::sync::Mutex;
use once_cell::sync::OnceCell;

const shop_id: i32 = 1; // 3DS
// const shop_id: i32 = 2; // Wii U?

// Used to avoid rate-limiting. Lower at your own risk.
const FETCH_DELAY: time::Duration = time::Duration::from_secs(1);

// List of countries that don't return an error on Samurai's news endpoint.
// Many of these only report empty content listings, though.
const REGIONS: &[&str] =
    &[ "AD", "AE", "AG", "AI", "AL", "AN", "AR", "AT", "AU", "AW", "AZ", "BA",
       "BB", "BE", "BG", "BM", "BO", "BR", "BS", "BW", "BZ", "CA", "CH", "CL",
       "CN", "CO", "CR", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "EC", "EE",
       "ER", "ES", "FI", "FR", "GB", "GD", "GF", "GG", "GI", "GP", "GR", "GT",
       "GY", "HK", "HN", "HR", "HT", "HU", "IE", "IL", "IM", "IN", "IS", "IT",
       "JE", "JM", "JP", "KN", "KR", "KY", "LC", "LI", "LS", "LT", "LU", "LV",
       "MC", "ME", "MK", "ML", "MQ", "MR", "MS", "MT", "MX", "MY", "MZ", "NA",
       "NE", "NI", "NL", "NO", "NZ", "PA", "PE", "PL", "PT", "PY", "RO", "RS",
       "RU", "SA", "SD", "SE", "SG", "SI", "SK", "SM", "SO", "SR", "SV", "SZ",
       "TC", "TD", "TR", "TT", "TW", "US", "UY", "VA", "VC", "VE", "VG", "VI",
       "ZA", "ZM", "ZW",
];

fn samurai_baseurl(region: &str) -> String {
    return "https://samurai.ctr.shop.nintendo.net/samurai/ws/".to_owned() + region;
}

fn ninja_baseurl(region: &str) -> String {
    return "https://ninja.ctr.shop.nintendo.net/ninja/ws/".to_owned() + region;
}

struct Locale {
    region: String,
    language: String,
}

// There are many duplicate resource references across titles/languages/regions,
// so cache the download urls already processed
static HTTP_HEADERS_LOG: OnceCell<Mutex<File>> = OnceCell::new();
static HTTP_HEADERS_SEPARATOR: &str = "--------------------------------------------------\n";

fn log_headers<U: reqwest::IntoUrl + Clone + std::fmt::Display>(url: U, headers: &reqwest::header::HeaderMap<reqwest::header::HeaderValue>) {
    let json = format!(concat!(
                "{{\n",
                "  \"url\": \"{}\",\n",
                "  \"response_headers\": {{\n",
                "    {}\n",
                "  }}\n",
                "}}\n",
                "{}"),
                url,
                headers.iter().map(|(name, value)| format!("\"{}\": \"{}\"", name, value.to_str().unwrap())).collect::<Vec<_>>().join(",\n    "),
                HTTP_HEADERS_SEPARATOR);
    let mut file = HTTP_HEADERS_LOG.get().unwrap().lock().unwrap();
    write!(file, "{}", json).unwrap();
    file.sync_data().unwrap();
}

async fn get_with_retry<U: reqwest::IntoUrl + Clone + std::fmt::Display>(client: &reqwest::Client, url: U) -> Result<String, reqwest::Error> {
    return get_with_retry_generic(&client.get(url.clone()), url, &|response: reqwest::Response| response.text()).await;
}

async fn get_with_retry_generic<U, C, F, Output>(request: &reqwest::RequestBuilder, url: U, continuation: C) -> Result<Output, reqwest::Error>
    where   U: reqwest::IntoUrl + Clone + std::fmt::Display,
            C: Fn(reqwest::Response) -> F,
            F: std::future::Future<Output = Result<Output, reqwest::Error>> {
    return loop {
        let err = match request.try_clone().unwrap().send().await {
                Ok(response) => {
                let headers = response.headers().clone();
                match continuation(response).await {
                    Ok(response_text) => {
                        let resource_cache = RESOURCE_CACHE.get().unwrap();
                        if matches!(resource_cache.lock().unwrap().get(&url.to_string()), None) {
                            // Add dummy entry to resource cache to avoid logging the same request twice
                            // (Note that logging itself happens in get_with_retry_generic)
                            resource_cache.lock().unwrap().insert(url.to_string(), 1);
                            log_headers(url, &headers);
                        }
                        break Ok(response_text)
                    },
                    Err(err) => err,
                }
            }
            Err(err) => err,
        };
        println!("  Got error {}, retrying in 10 seconds", err);
        thread::sleep(time::Duration::from_secs(10));
    }
}

async fn fetch_endpoint(client: &reqwest::Client, endpoint: &str, locale: &Locale) -> Result<String, reqwest::Error> {
    let resp = get_with_retry(client, format!("{}/{}?shop_id={}&lang={}", samurai_baseurl(&locale.region), endpoint, shop_id, locale.language)).await?;
    Ok(resp)
}

#[derive(Deserialize)]
struct NodeThumbnail {
    #[serde(rename = "@url")]
    url: String,
}

#[derive(Deserialize)]
struct NodeThumbnails {
    thumbnail: Vec<NodeThumbnail>,
}

impl Default for NodeThumbnails {
    fn default() -> Self { Self { thumbnail: Vec::new() } }
}

#[derive(Deserialize)]
struct NodeRatingIcon {
    #[serde(rename = "@url")]
    url: String
}

#[derive(Deserialize)]
struct NodeRatingIcons {
    icon: Vec<NodeRatingIcon>
}

#[derive(Deserialize)]
struct NodeRating {
    icons: NodeRatingIcons
}

#[derive(Deserialize)]
struct NodeRatingInfo {
    rating: NodeRating
}

#[derive(Deserialize)]
struct DemoTitle {
    #[serde(rename = "@id")]
    id: String,
    name: String,

    // Optional e.g. when embedded in title 50010000047595 for shop_id=2
    icon_url: Option<String>,

    // Not present when nested in a <title> tag
    rating_info: Option<NodeRatingInfo>,
}

#[derive(Deserialize)]
struct DemoTitles {
    demo_title: Vec<DemoTitle>
}

#[derive(Deserialize)]
struct NodeScreenshotImageUrl {
    // For 3DS content, there's two of these: one with type=upper and one with type=lower.
    // Wii U doesn't use this attribute
    #[serde(rename = "@type")]
    screen: Option<String>,

    #[serde(rename = "$value")]
    url: String,
}

#[derive(Deserialize)]
struct NodeScreenshot {
    image_url: Vec<NodeScreenshotImageUrl>,

    // Wii U titles (shop_id=2) use this to store a thumbnail per screenshot
    #[serde(default)]
    thumbnail_url: Vec<NodeScreenshotImageUrl>,
}

#[derive(Deserialize)]
struct NodeScreenshots {
    screenshot: Vec<NodeScreenshot>,
}

impl Default for NodeScreenshots {
    fn default() -> Self { Self { screenshot: Vec::new() } }
}

#[derive(Deserialize)]
struct NodeTitle {
    #[serde(rename = "@id")]
    id: String,

    name: String,
    // Not present e.g. in title 50010000047595 with shop_id = 2
    icon_url: Option<String>,
    // Not present when shop_id != 1
    banner_url: Option<String>,

    #[serde(default)]
    thumbnails: NodeThumbnails,

    rating_info: Option<NodeRatingInfo>,

    // Screenshots are only listed in detail views
    #[serde(default)]
    screenshots: NodeScreenshots,

    // Add-On Content (=DLC)
    aoc_available: bool,

    // If true, demo_titles is non-empty (for detailed title pages, only)
    demo_available: bool,
    demo_titles: Option<DemoTitles>,

    movies: Option<NodeMovies>,
}

#[derive(Deserialize)]
struct NodeMovies {
    movie: Vec<NodeMovie>,
}

#[derive(Deserialize)]
struct NodeMovieFile {
    movie_url: String,

    // ""3d" for 3D videos, "2d" otherwise
    dimension: String,
}

#[derive(Deserialize)]
struct NodeMovieFiles {
    // NOTE: May be empty (e.g. movie 20040000033107)
    #[serde(default)]
    file: Vec<NodeMovieFile>,
}

#[derive(Deserialize)]
struct NodeMovie {
    #[serde(rename = "@id")]
    id: String,

    name: String,
    // Normally present unless this content was taken down from eShop
    banner_url: Option<String>,
    // Normally present unless this content was taken down from eShop
    thumbnail_url: Option<String>,

    rating_info: Option<NodeRatingInfo>,

    files: NodeMovieFiles,
}

#[derive(Deserialize)]
struct NodeDirectory {
    #[serde(rename = "@id")]
    id: String,

    name: String,
    icon_url: Option<String>,
    banner_url: String,

    contents: Option<NodeContents>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum NodeTitleOrMovie {
    Title(NodeTitle),
    Movie(NodeMovie),
}

#[derive(Deserialize)]
struct TitleDocument {
    title: NodeTitle,
}

#[derive(Deserialize)]
struct DemoDocument {
    // Demo pages wrap the demo node in a "<content>" tag
    content: DemoDocumentContent,
}

#[derive(Deserialize)]
struct DemoDocumentContent {
    demo: DemoTitle,
}

#[derive(Deserialize)]
struct MovieDocument {
    movie: NodeMovie,
}

#[derive(Deserialize)]
struct DirectoryDocument {
    directory: NodeDirectory,
}

#[derive(Deserialize)]
struct NodeRanking {
    #[serde(rename = "@id")]
    id: String,

    contents: Option<NodeContents>,
}

#[derive(Deserialize)]
struct RankingDocument {
    ranking: NodeRanking,
}

#[derive(Deserialize)]
struct NodeLanguage {
    iso_code: String,
    name: String,
}

#[derive(Deserialize)]
struct NodeLanguages {
    language: Vec<NodeLanguage>,
}

#[derive(Deserialize)]
struct LanguagesDocument {
    languages: NodeLanguages,
}

#[derive(Deserialize)]
struct NodeContent {
    #[serde(rename = "@index")]
    index: String,

    #[serde(rename = "$value")]
    title_or_movie: NodeTitleOrMovie,
}

// Work around Serde's lack of support for parsing number literals in defaults
fn default_to_one() -> usize { 1 }

#[derive(Deserialize)]
struct NodeContents {
    // Length and offset are optional if the entire list is included
    #[serde(rename = "@length")]
    length: Option<usize>,
    #[serde(rename = "@offset")]
    offset: Option<usize>,
    // Total size is optional if it's 1
    #[serde(rename = "@total", default = "default_to_one")]
    total: usize,

    // Some regions report no contents
    #[serde(default)]
    content: Vec<NodeContent>,
}

#[derive(Deserialize)]
struct NodeEshop {
    contents: NodeContents,
}

#[derive(Deserialize)]
struct NodeEshopDirectoryList {
    // Some regions report no directories
    #[serde(default)]
    directory: Vec<NodeDirectory>,
}

#[derive(Deserialize)]
struct NodeEshopDirectories {
    directories: NodeEshopDirectoryList,
}

#[derive(Deserialize)]
struct NodeEshopRankingList {
    ranking: Vec<NodeRanking>,
}
#[derive(Deserialize)]
struct NodeEshopRankings {
    rankings: NodeEshopRankingList,
}

#[derive(Clone, Copy, PartialEq)]
enum ContentType {
    Title,
    Movie,
    // NOTE: The "contents" endpoint covers movies, but not demos
    Demo,
}

async fn fetch_directory_list(client: &reqwest::Client, locale: &Locale) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let resp = get_with_retry(client, format!(  "{}/directories?shop_id={}&lang={}",
                                    samurai_baseurl(&locale.region), shop_id, &locale.language)).await?;
    let doc: Result<NodeEshopDirectories, _> = quick_xml::de::from_str(&resp);
    match doc {
        Ok(doc) =>
            Ok(doc.directories.directory.into_iter().map(|dir| {
                println!("Directory {}: {}", dir.id, dir.name.replace("\n", " ").replace("<br>", ""));
                dir.id
            }).collect()),
        // Some regions return an error page for this, but empty lists on other types of content. Just return an empty list here too, hence.
        Err(_) => Ok(Vec::new())
    }
}

async fn fetch_content_list(client: &reqwest::Client, endpoint: EndPoint, locale: &Locale)
    -> Result<Vec<(ContentType, String)>, Box<dyn std::error::Error>> {
    let mut content_list = Vec::new();

    let mut offset = 0;
    let mut full_list = Vec::new();

    fs::create_dir_all(format!("samurai/{}/{}/paginated/", locale.region, locale.language)).unwrap();

    loop {
        let resp = get_with_retry(client, format!(  "{}/{}?offset={}&shop_id={}&lang={}",
                                        samurai_baseurl(&locale.region), endpoint, offset, shop_id, &locale.language)).await?;

        let mut file = File::create(format!("samurai/{}/{}/paginated/contents%3Foffset%3D{}", locale.region, locale.language, offset)).unwrap();
        write!(file, "{}", &resp)?;

        let doc: NodeEshop = quick_xml::de::from_str(&resp).unwrap();

        if doc.contents.total == 0 {
            println!("No contents available");
            break;
        }

        println!("Contents {}-{}, {} total", offset, (offset + doc.contents.length.unwrap_or(doc.contents.total - offset)).saturating_sub(1), doc.contents.total);
        assert_eq!(doc.contents.offset.unwrap_or(0), offset);
        assert_eq!(doc.contents.content.len(), doc.contents.length.unwrap_or(doc.contents.total));
        assert!(doc.contents.content.len() <= doc.contents.total);
        if doc.contents.total > 0 {
            assert_eq!(doc.contents.content[0].index, (offset + 1).to_string());
        }
        for content in &doc.contents.content {
            match &content.title_or_movie {
                NodeTitleOrMovie::Title(title) => {
                    println!("  Title {}: {}", title.id, title.name.replace("\n", " ").replace("<br>", ""));
                    content_list.push((ContentType::Title, title.id.clone()));
                },
                NodeTitleOrMovie::Movie(movie) => {
                    println!("  Movie {}: {}", movie.id, movie.name.replace("\n", " ").replace("<br>", ""));
                    content_list.push((ContentType::Movie, movie.id.clone()));
                }
            }
        }

        // Extract <contents> body and its surrounding bits, while dropping the opening <contents> tag.
        // This makes it easy to merge the included <content> tags under a single, manually written <contents> node.
        let (doc_header, contents_and_footer) = resp.split_at(resp.find("<contents ").unwrap());
        let (_, contents_and_footer) = contents_and_footer.split_once(">").unwrap();
        let (contents, doc_footer) = contents_and_footer.split_at(contents_and_footer.find("</contents>").unwrap());
        if full_list.is_empty() {
            full_list.push(doc_header.to_owned());
            full_list.push(format!("<contents length=\"{}\" offset=\"0\" total=\"{}\">", doc.contents.total, doc.contents.total));
        }
        full_list.push(contents.to_owned());

        offset += doc.contents.content.len();
        if offset == doc.contents.total {
            full_list.push(doc_footer.to_owned());
            break;
        }
        thread::sleep(FETCH_DELAY);
    }

    let mut file = File::create(format!("samurai/{}/{}/contents", locale.region, locale.language)).unwrap();
    for contents in full_list {
        write!(file, "{}\n", contents)?;
    }

    Ok(content_list)
}

async fn handle_content<T: DeserializeOwned>(client: &reqwest::Client, content_id: &str, content_type: ContentType, locale: &Locale, omit_ninja: bool) -> Result<T, Box<dyn std::error::Error>> {
    let content_type_name = match content_type {
        ContentType::Title => "title",
        ContentType::Movie => "movie",
        ContentType::Demo => "demo",
    };
    let url = format!(  "{}/{}/{}?shop_id={}&lang={}", samurai_baseurl(&locale.region), content_type_name, content_id, shop_id, &locale.language);
    let resp = get_with_retry(client, url).await?;

    fs::create_dir_all(format!("samurai/{}/{}/{}", locale.region, locale.language, content_type_name)).unwrap();
    let mut file = File::create(format!("samurai/{}/{}/{}/{}", locale.region, locale.language, content_type_name, content_id)).unwrap();
    write!(file, "{}", resp)?;

    if !omit_ninja {
        // Fetch mapping from content id to title id
        if content_type == ContentType::Title ||
           content_type == ContentType::Demo {
            let ecinfo_resp = get_with_retry(client, format!(   "{}/title/{}/ec_info?shop_id={}&lang={}",
                                                    ninja_baseurl(&locale.region), content_id, shop_id, &locale.language)).await?;
            // Both titles and demos are exposed through the "title" endpoint
            fs::create_dir_all(format!("ninja/{}/{}/title/{}", locale.region, locale.language, content_id)).unwrap();
            let mut file = File::create(format!("ninja/{}/{}/title/{}/ec_info", locale.region, locale.language, content_id)).unwrap();
            write!(file, "{}", ecinfo_resp)?;
        }

        // Fetch price information
        if content_type == ContentType::Title {
            // NOTE: Just returns "<eshop><online_prices/></eshop>" for arguments that are title ids but not purchasable (e.g. movies)
            let price_resp = get_with_retry(client, format!("{}/titles/online_prices?shop_id={}&lang={}&title[]={}",
                                                    ninja_baseurl(&locale.region), shop_id, &locale.language, content_id)).await?;
            fs::create_dir_all(format!("ninja/{}/{}/titles", locale.region, locale.language)).unwrap();
            let mut file = File::create(format!("ninja/{}/{}/titles/online_prices%3Ftitle%5B%5D%3D{}", locale.region, locale.language, content_id)).unwrap();
            write!(file, "{}", price_resp)?;
        }
    }

    Ok(quick_xml::de::from_str(&resp).unwrap())
}

async fn handle_directory_content(client: &reqwest::Client, directory_id: &str, locale: &Locale) -> Result<DirectoryDocument, Box<dyn std::error::Error>> {
    fs::create_dir_all(format!("samurai/{}/{}/directory/paginated", locale.region, locale.language)).unwrap();

    let mut directory_info = None;

    let mut offset = 0;
    let mut full_list = Vec::new();
    loop {
        let resp = get_with_retry(client, format!(  "{}/directory/{}?offset={}&shop_id={}&lang={}",
                                        samurai_baseurl(&locale.region), directory_id, offset, shop_id, &locale.language)).await?;

        let mut file = File::create(format!("samurai/{}/{}/directory/paginated/{}%3Foffset%3D{}", locale.region, locale.language, directory_id, offset)).unwrap();
        write!(file, "{}", &resp)?;

        let doc: DirectoryDocument = quick_xml::de::from_str(&resp).unwrap();

        let contents = doc.directory.contents.as_ref().unwrap();

        println!("  Directory contents {}-{}, {} total", offset, offset + contents.length.unwrap_or(contents.total) - 1, contents.total);
        assert_eq!(contents.offset.unwrap_or(0), offset);
        assert_eq!(contents.content.len(), contents.length.unwrap_or(contents.total));
        assert!(contents.content.len() <= contents.total);
        if contents.total > 0 {
            assert_eq!(contents.content[0].index, (offset + 1).to_string());
        }
        for content in &contents.content {
            match &content.title_or_movie {
                NodeTitleOrMovie::Title(title) => {
                    println!("    Title {}: {}", title.id, title.name.replace("\n", " ").replace("<br>", ""));
                },
                NodeTitleOrMovie::Movie(movie) => {
                    println!("    Movie {}: {}", movie.id, movie.name.replace("\n", " ").replace("<br>", ""));
                }
            }
        }

        offset += contents.content.len();
        let total_contents = contents.total;

        // Extract <contents> body and its surrounding bits, while dropping the opening <contents> tag.
        // This makes it easy to merge the included <content> tags under a single, manually written <contents> node.
        let (doc_header, contents_and_footer) = resp.split_at(resp.find("<contents ").unwrap());
        let (_, contents_and_footer) = contents_and_footer.split_once(">").unwrap();
        let (contents, doc_footer) = contents_and_footer.split_at(contents_and_footer.find("</contents>").unwrap());
        if full_list.is_empty() {
            full_list.push(doc_header.to_owned());
            full_list.push(format!("<contents length=\"{}\" offset=\"0\" total=\"{}\">", total_contents, total_contents));
        }
        full_list.push(contents.to_owned());

        if directory_info.is_none() {
            directory_info = Some(doc);
        } else {
            let previous_contents = directory_info.as_mut().unwrap().directory.contents.as_mut().unwrap();
            previous_contents.content.extend(doc.directory.contents.unwrap().content.into_iter());
        }

        if offset == total_contents {
            full_list.push(doc_footer.to_owned());
            break;
        }
        thread::sleep(FETCH_DELAY);
    }

    let mut file = File::create(format!("samurai/{}/{}/directory/{}", locale.region, locale.language, directory_id)).unwrap();
    for contents in full_list {
        write!(file, "{}\n", contents)?;
    }

    Ok(directory_info.unwrap())
}

async fn handle_ranking_content(client: &reqwest::Client, ranking_id: &str, locale: &Locale) -> Result<RankingDocument, Box<dyn std::error::Error>> {
    fs::create_dir_all(format!("samurai/{}/{}/ranking/paginated", locale.region, locale.language)).unwrap();

    let mut ranking_info = None;

    let mut offset = 0;
    let mut full_list = Vec::new();
    loop {
        let resp = get_with_retry(client, format!(  "{}/ranking/{}?offset={}&shop_id={}&lang={}",
                                        samurai_baseurl(&locale.region), ranking_id, offset, shop_id, &locale.language)).await?;

        let mut file = File::create(format!("samurai/{}/{}/ranking/paginated/{}%3Foffset%3D{}", locale.region, locale.language, ranking_id, offset)).unwrap();
        write!(file, "{}", &resp)?;

        let doc: RankingDocument = quick_xml::de::from_str(&resp).unwrap();

        let dummy = NodeContents { content: Vec::new(), length: Some(0), total: 0, offset: Some(0) };
        let contents = doc.ranking.contents.as_ref().unwrap_or(&dummy);
        if contents.total == 0 {
            ranking_info = Some(doc);
            full_list.push(resp);
            break;
        }

        println!("  Ranking contents {}-{}, {} total", offset, offset + contents.length.unwrap_or(contents.total) - 1, contents.total);
        assert_eq!(contents.offset.unwrap_or(0), offset);
        assert_eq!(contents.content.len(), contents.length.unwrap_or(contents.total));
        assert!(contents.content.len() <= contents.total);
        // NOTE: For rankings, the reported "index" always starts at 1 even when results are reported across multiple pages
        for content in &contents.content {
            match &content.title_or_movie {
                NodeTitleOrMovie::Title(title) => {
                    println!("    Title {}: {}", title.id, title.name.replace("\n", " ").replace("<br>", ""));
                },
                NodeTitleOrMovie::Movie(movie) => {
                    println!("    Movie {}: {}", movie.id, movie.name.replace("\n", " ").replace("<br>", ""));
                }
            }
        }

        offset += contents.content.len();
        let total_contents = contents.total;

        // Extract <contents> body and its surrounding bits, while dropping the opening <contents> tag.
        // This makes it easy to merge the included <content> tags under a single, manually written <contents> node.
        let (doc_header, contents_and_footer) = resp.split_at(resp.find("<contents ").unwrap());
        let (_, contents_and_footer) = contents_and_footer.split_once(">").unwrap();
        let (contents, doc_footer) = contents_and_footer.split_at(contents_and_footer.find("</contents>").unwrap());
        if full_list.is_empty() {
            full_list.push(doc_header.to_owned());
            full_list.push(format!("<contents length=\"{}\" offset=\"0\" total=\"{}\">", total_contents, total_contents));
        }
        full_list.push(contents.to_owned());

        if ranking_info.is_none() {
            ranking_info = Some(doc);
        } else {
            let previous_contents = ranking_info.as_mut().unwrap().ranking.contents.as_mut().unwrap();
            previous_contents.content.extend(doc.ranking.contents.unwrap().content.into_iter());
        }

        if offset == total_contents {
            full_list.push(doc_footer.to_owned());
            break;
        }
        thread::sleep(FETCH_DELAY);
    }

    let mut file = File::create(format!("samurai/{}/{}/ranking/{}", locale.region, locale.language, ranking_id)).unwrap();
    for contents in full_list {
        write!(file, "{}\n", contents)?;
    }

    Ok(ranking_info.unwrap())
}

// There are many duplicate resource references across titles/languages/regions,
// so cache the download urls and content sizes
static RESOURCE_CACHE: OnceCell<Mutex<HashMap<String, u64>>> = OnceCell::new();

async fn fetch_resource(client: &reqwest::Client, resource_name: &str, url: &str) -> Result<(), Box<dyn std::error::Error>> {
    let filename = url_to_filename(url);

    let resource_cache = RESOURCE_CACHE.get().unwrap();
    let cached_size = *resource_cache.lock().unwrap().get(&url.to_string()).unwrap_or(&0);
    println!("  Fetching {} from {}{}", resource_name, url, if cached_size != 0 { format!(" ({} KiB, cached)", cached_size / 1024) } else { "".to_string() });
    if cached_size != 0 && Some(cached_size) == fs::metadata(&filename).map(|m| m.len()).ok() {
        return Ok(());
    }

    let data = loop {
        let response = client.get(url).send().await;
        let err = match response {
            Ok(response) => {
                let headers = response.headers().clone();
                if let Ok(existing_file) = fs::metadata(&filename) {
                    if Some(existing_file.len()) == response.content_length() {
                        println!("    ... already exists on disk ({} KiB), skipping", response.content_length().unwrap() / 1024);
                        log_headers(url, &headers);
                        resource_cache.lock().unwrap().insert(url.to_string(), response.content_length().unwrap());
                        return Ok(());
                    }
                }

                match response.bytes().await {
                    Ok(bytes) => {
                        resource_cache.lock().unwrap().insert(url.to_string(), bytes.len() as u64);
                        log_headers(url, &headers);
                        break bytes
                    },
                    Err(err) => err,
                }
            },
            Err(err) => err,
        };
        println!("  Got error {}, retrying in 10 seconds", err);
        thread::sleep(time::Duration::from_secs(10));
    };

    File::create(filename)?.write_all(&data)?;

    thread::sleep(FETCH_DELAY);

    Ok(())
}

enum EndPoint {
    Contents,
    Titles,
    Movies,
    News,
    Telops,
    Directories,
    Genres,
    Publishers,
    PublisherContacts,
    Platforms,
    Languages,
    Rankings,
    SearchCategory,
}

#[derive(clap::Args)]
#[clap(global_setting(clap::AppSettings::DeriveDisplayOrder))]
struct FetchMetadataArgs {
    /// Path to ctr-common-1 certificate in PEM format (see Readme)
    #[clap(long, group = "cert-group")]
    cert: Option<String>,

    /// Skip data provided from "ninja" servers (prices, title ids, ...)
    #[clap(long, action, group = "cert-group")]
    omit_ninja_contents: bool,
}

#[derive(clap::Args)]
struct FetchMediaArgs {
    /// Download associated video files
    #[clap(long, action)]
    fetch_videos: bool,

    /// Same as fetch-videos but needed to confirm unrestricted download of all videos
    #[clap(long, action, hide=true)]
    fetch_all_videos: bool,
}

#[derive(clap::Args)]
struct FetchAllArgs {
    #[clap(flatten)]
    metadata: FetchMetadataArgs,

    #[clap(flatten)]
    media: FetchMediaArgs,
}

#[derive(clap::Args)]
struct ConvertMediaArgs {
    /// Only convert a specific moflex file
    #[clap(long, value_name = "PATH")]
    filename: Option<String>
}

#[derive(clap::Subcommand)]
enum SubCommand {
    /// Fetch general title information
    FetchMetadata(FetchMetadataArgs),
    /// Fetch images and (optionally) videos for previously fetched metadata
    FetchMedia(FetchMediaArgs),
    /// Fetch both metadata and media
    FetchAll(FetchAllArgs),
    /// Convert moflex video files to mp4
    ConvertMedia(ConvertMediaArgs),
}

#[derive(Parser)]
struct Args {
    #[clap(subcommand)]
    command: SubCommand,

    /// Only fetch data for the given title
    #[clap(long = "title", value_name = "ID", global = true, display_order = 3)]
    title_id: Option<String>,

    /// Only fetch data for the given movie
    #[clap(long = "movie", value_name = "ID", global = true, display_order = 4)]
    movie_id: Option<String>,

    /// Only fetch data for the given directory and its contents
    #[clap(long = "directory", value_name = "ID", global = true, display_order = 5)]
    directory_id: Option<String>,

    /// Comma-delimited list of eShop regions to fetch from
    #[clap(long, possible_values = REGIONS, global = true, use_delimiter = true)]
    regions: Vec<String>,
}

impl fmt::Display for EndPoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", match self {
            EndPoint::Contents => "contents",
            EndPoint::Titles => "titles",
            EndPoint::Movies => "movies",
            EndPoint::News => "news",
            EndPoint::Telops => "telops",
            EndPoint::Directories => "directories",
            EndPoint::Genres => "genres",
            EndPoint::Publishers => "publishers",
            EndPoint::PublisherContacts => "publishers/contacts",
            EndPoint::Platforms => "platforms",
            EndPoint::Languages => "languages",
            EndPoint::Rankings => "rankings",
            EndPoint::SearchCategory => "searchcategory"
        })
    }
}

fn url_to_filename(url: &str) -> String {
    let url = url.strip_prefix("https://").unwrap();
    let base_url = &url[0..url.find("/").unwrap() + 1];
    let path = url.strip_prefix(base_url).unwrap();
    match base_url {
        "kanzashi-ctr.cdn.nintendo.net/" => { format!("kanzashi/{}", path.strip_prefix("i/").unwrap()) },
        "kanzashi-wup.cdn.nintendo.net/" => { format!("kanzashi/{}", path.strip_prefix("i/").unwrap()) },
        _ => panic!("Unrecognized resource URL \"{}\"", url)
    }
}

fn movie_url_to_filename(url: &str) -> String {
    format!("kanzashi-movie/{}", url.strip_prefix("https://kanzashi-movie-ctr.cdn.nintendo.net/m/").unwrap())
}

async fn fetch_movie_file(client: &reqwest::Client, file: &NodeMovieFile) -> Result<(), Box<dyn std::error::Error>> {
    let resource_cache = RESOURCE_CACHE.get().unwrap();
    let cached_size = *resource_cache.lock().unwrap().get(&file.movie_url).unwrap_or(&0);
    println!("  Fetching movie from {}{}", file.movie_url, if cached_size != 0 { format!(" ({} MiB, cached)", cached_size / 1024 / 1024) } else { "".to_string() });
    if cached_size != 0 {
        return Ok(());
    }

    let filename = movie_url_to_filename(&file.movie_url);
    assert_eq!("moflex", std::path::Path::new(&filename).extension().unwrap());

    // Skip if content size matches the moflex on disk
    if let Ok(existing_file) = fs::metadata(&filename) {
        let response = client.get(&file.movie_url).send().await?;
        let content_length = response.content_length();
        if Some(existing_file.len()) == content_length {
            println!("    ... already exists on disk ({} MiB), skipping", content_length.unwrap() / 1024 / 1024);
            log_headers(&file.movie_url, &response.headers());
            resource_cache.lock().unwrap().insert(file.movie_url.clone(), content_length.unwrap());
            return Ok(())
        }
    }

    let movie_data = get_with_retry_generic(&client.get(&file.movie_url), file.movie_url.clone(), &|response: reqwest::Response| response.bytes()).await?;
    File::create(&filename)?.write_all(&movie_data)?;

    thread::sleep(FETCH_DELAY * 10);

    return Ok(());
}

async fn fetch_metadata(client: &reqwest::Client, locale: &Locale, args: &Args, metadata_args: &FetchMetadataArgs) -> Result<(), Box<dyn std::error::Error>> {
    // NOTE: We're fetching languages *again* here since language names are localized
    for endpoint in vec![EndPoint::News, EndPoint::Telops, EndPoint::Directories, EndPoint::Genres, EndPoint::Publishers, EndPoint::PublisherContacts, EndPoint::Platforms, EndPoint::SearchCategory, EndPoint::Languages, EndPoint::Rankings] {
        println!("Fetching endpoint {}", endpoint);
        let data = fetch_endpoint(&client, &format!("{}", endpoint), &locale).await?;
        let filename = format!( "samurai/{}/{}/{}", locale.region, locale.language,
                                if matches!(endpoint, EndPoint::PublisherContacts) { "publishers_/contacts".to_owned() } else { endpoint.to_string() });
        let mut file = File::create(filename).unwrap();
        write!(file, "{}", data)?;

        if matches!(endpoint, EndPoint::Rankings) {
            let parsed_xml: NodeEshopRankings = quick_xml::de::from_str(&data).unwrap();
            for ranking in parsed_xml.rankings.ranking {
                let _: RankingDocument = handle_ranking_content(&client, &ranking.id, &locale).await?;
            }
        }
    }

    let (mut title_ids, mut movie_ids, mut directory_ids) = match (&args.title_id, &args.movie_id, &args.directory_id) {
        (None, None, None) => {
            let mut title_ids = Vec::new();
            let mut movie_ids = Vec::new();
            for content in fetch_content_list(&client, EndPoint::Contents, &locale).await? {
                match content {
                    (ContentType::Title, id) => title_ids.push(id),
                    (ContentType::Movie, id) => movie_ids.push(id),

                    // "contents" endpoint only contains titles and movies
                    (ContentType::Demo, _) => panic!("Unexpected demo title in contents list"),
                }
            }
            let directory_ids = Vec::<_>::from_iter(fetch_directory_list(&client, &locale).await?.into_iter());
            (title_ids, movie_ids, directory_ids)
        },
        _ => (args.title_id.clone().into_iter().collect::<Vec<_>>(),
            args.movie_id.clone().into_iter().collect::<Vec<_>>(),
            args.directory_id.clone().into_iter().collect::<Vec<_>>())
    };

    directory_ids.sort_unstable();
    for (index, directory_id) in directory_ids.iter().enumerate() {
        println!("Fetching metadata for directory {} ({} out of {})", directory_id, index + 1, directory_ids.len());
        let directory: DirectoryDocument = handle_directory_content(&client, &directory_id, &locale).await?;
        let directory = directory.directory;
        assert!(directory.contents.is_some());
        for content in directory.contents.unwrap().content {
            match content.title_or_movie {
                NodeTitleOrMovie::Title(title) => if !title_ids.contains(&title.id) { title_ids.push(title.id) },
                NodeTitleOrMovie::Movie(movie) => if !movie_ids.contains(&movie.id) { movie_ids.push(movie.id) },
            }
        }
    }

    title_ids.sort_unstable();
    title_ids.dedup();
    for (index, title_id) in title_ids.iter().enumerate() {
        println!("Fetching metadata for title {} ({} out of {})", title_id, index + 1, title_ids.len());
        let content: TitleDocument = handle_content(&client, &title_id, ContentType::Title, &locale, metadata_args.omit_ninja_contents).await?;
        let title = content.title;

        if title.aoc_available {
            println!("  Fetching DLC list");
            let dlc_resp = get_with_retry(&client, format!("{}/title/{}/aocs?shop_id={}&lang={}",
                                                    samurai_baseurl(&locale.region), title_id, shop_id, &locale.language)).await?;
            fs::create_dir_all(format!("samurai/{}/{}/title/aocs", locale.region, locale.language)).unwrap();
            let mut file = File::create(format!("samurai/{}/{}/title/aocs/{}", locale.region, locale.language, title_id)).unwrap();
            write!(file, "{}", dlc_resp)?;
        }

        if title.demo_available {
            assert!(title.demo_titles.is_some());
            for demo_title in title.demo_titles.as_ref().unwrap().demo_title.iter() {
                println!("  Fetching metadata for demo {}", demo_title.id);
                let _: DemoDocument = handle_content(&client, &demo_title.id, ContentType::Demo, &locale, metadata_args.omit_ninja_contents).await?;
            }
        }

        // Add referenced movie trailers
        for movie in title.movies.iter().map(|m| &m.movie).flatten() {
            movie_ids.push(movie.id.clone());
        }

        thread::sleep(FETCH_DELAY);
    }

    movie_ids.sort_unstable();
    movie_ids.dedup();
    for (index, movie_id) in movie_ids.iter().enumerate() {
        println!("Fetching metadata for movie {} ({} out of {})", movie_id, index + 1, movie_ids.len());
        let _: MovieDocument = handle_content(&client, &movie_id, ContentType::Movie, &locale, metadata_args.omit_ninja_contents).await?;
    }

    Ok(())
}

async fn fetch_media_resources(client: &reqwest::Client, region: &str, args: &Args, fetch_args: &FetchMediaArgs) -> Result<(), Box<dyn std::error::Error>> {
    let dir_entries = std::fs::read_dir(format!("samurai/{}", region)).into_iter().flatten().flatten();

    for subdir in dir_entries.filter(|f| f.file_type().unwrap().is_dir()) {
        println!("Gathering media resources for region {} / language {}", region, subdir.file_name().to_str().unwrap());

        let contained_files_iter = |path| {
            std::fs::read_dir(path)
                    .into_iter()
                    .flatten()
                    .flatten()
                    .filter(|f| f.file_type().unwrap().is_file())
        };

        let icons_from_rating_info = |rating_info: Option<NodeRatingInfo>| if rating_info.is_some() { rating_info.unwrap().rating.icons.icon } else { Vec::new() };

        let constrained_fetch = args.directory_id.is_some() || args.title_id.is_some() || args.movie_id.is_some();
        let build_contents_list = |content_name, exclude_list: Vec<_>| {
            Vec::<_>::from_iter(
                contained_files_iter(subdir.path().join(content_name))
                .filter(|d| !constrained_fetch || exclude_list.iter().any(|item| *item == d.file_name().to_string_lossy().to_string()))
                .map(|d| d.path())
            )
        };

        let mut title_set = Vec::<_>::from_iter(args.title_id.clone().into_iter());
        let mut movie_set = Vec::<_>::from_iter(args.movie_id.clone().into_iter());

        let mut directory_set = build_contents_list("directory", Vec::<_>::from_iter(args.directory_id.clone().into_iter()));
        directory_set.sort_unstable();
        for (dir_index, directory) in directory_set.iter().enumerate() {
            println!(" Directory {} ({} out of {})", &directory.display(), dir_index + 1, directory_set.len());
            let parsed_xml: DirectoryDocument = quick_xml::de::from_str(&String::from_utf8(fs::read(directory).unwrap()).unwrap()).unwrap();
            let directory = parsed_xml.directory;
            assert!(directory.contents.is_some());

            println!("  Name: {}", &directory.name.replace("\n", " "));

            if let Some(icon_url) = directory.icon_url {
                fetch_resource(&client, "icon", &icon_url).await?;
            }
            fetch_resource(&client, "banner", &directory.banner_url).await?;

            // Include titles and movies referenced by this directory
            if constrained_fetch {
                for content in directory.contents.into_iter().map(|c| c.content).flatten() {
                    match content.title_or_movie {
                        NodeTitleOrMovie::Title(title) => { title_set.push(title.id); },
                        NodeTitleOrMovie::Movie(movie) => { movie_set.push(movie.id); },
                    };
                }
            }
        }

        let mut demo_set = Vec::new();
        let mut title_set = build_contents_list("title", title_set);
        title_set.sort_unstable();
        title_set.dedup();
        for (title_index, title) in title_set.iter().enumerate() {
            println!(" Title {} ({} out of {})", &title.display(), title_index + 1, title_set.len());
            let parsed_xml: TitleDocument = quick_xml::de::from_str(&String::from_utf8(fs::read(title).unwrap()).unwrap()).unwrap();
            let title = parsed_xml.title;

            println!("  Name: {}", &title.name.replace("\n", " "));

            if let Some(icon_url) = title.icon_url {
                fetch_resource(&client, "icon", &icon_url).await?;
            }
            if let Some(banner_url) = title.banner_url {
                fetch_resource(&client, "banner", &banner_url).await?;
            }
            for thumbnail in title.thumbnails.thumbnail {
                fetch_resource(&client, "thumbnail", &thumbnail.url).await?;
            }
            for rating_icon in icons_from_rating_info(title.rating_info) {
                fetch_resource(&client, "rating icon", &rating_icon.url).await?;
            }

            for screenshot in title.screenshots.screenshot {
                for image_url in screenshot.image_url {
                    let resource_name = match image_url.screen {
                        None => "screenshot".to_string(),
                        Some(screen) => format!("{} screenshot", &screen),
                    };
                    fetch_resource(&client, &resource_name, &image_url.url).await?;
                }
                for thumbnail in screenshot.thumbnail_url {
                    fetch_resource(&client, "thumbnail", &thumbnail.url).await?;
                }
            }
            // TODO: urls, alternate_rating_image_url

            for movie in title.movies.map(|c| c.movie).unwrap_or_default() {
                if let Some(banner_url) = movie.banner_url {
                    fetch_resource(&client, "banner", &banner_url).await?;
                }
                if let Some(thumbnail_url) = movie.thumbnail_url {
                    fetch_resource(&client, "thumbnail", &thumbnail_url).await?;
                }

                for rating_icon in icons_from_rating_info(movie.rating_info) {
                    fetch_resource(&client, "rating icon", &rating_icon.url).await?;
                }

                if fetch_args.fetch_videos {
                    fs::create_dir_all(format!("kanzashi-movie")).unwrap();
                    for file in movie.files.file {
                        fetch_movie_file(&client, &file).await?;
                    }
                }
            }

            if title.demo_available {
                for demo_title in &title.demo_titles.as_ref().unwrap().demo_title {
                    demo_set.push(demo_title.id.clone());

                    let demo_path = subdir.path().join("demo").join(&demo_title.id);
                    if !demo_path.exists() {
                        println!("  WARNING: Title references demo {}, but there is no metadata at {}", demo_title.id, demo_path.display());
                        println!("  -------- Press Enter to continue --------");
                        use std::io::Read;
                        let _ = std::io::stdin().read(&mut [0u8]);
                        println!("           Continuing in 5 seconds...");
                        thread::sleep(time::Duration::from_secs(5));
                    }
                }
            }
        }

        let mut demo_set = build_contents_list("demo", demo_set);
        demo_set.sort_unstable();
        demo_set.dedup();
        for (demo_index, demo) in demo_set.iter().enumerate() {
            println!(" Demo {} ({} out of {})", &demo.display(), demo_index + 1, demo_set.len());

            let parsed_xml: DemoDocument = quick_xml::de::from_str(&String::from_utf8(fs::read(demo).unwrap()).unwrap()).unwrap();
            let demo = parsed_xml.content.demo;

            if let Some(icon_url) = demo.icon_url {
                fetch_resource(&client, "icon", &icon_url).await?;
            }
            for rating_icon in icons_from_rating_info(demo.rating_info) {
                fetch_resource(&client, "rating icon", &rating_icon.url).await?;
            }
            // NOTE: There are no demos with associated videos, banners, or thumbnails
        }

        let mut movie_set = build_contents_list("movie", movie_set);
        movie_set.sort_unstable();
        movie_set.dedup();
        for (movie_index, movie) in movie_set.iter().enumerate() {
            println!(" Movie {} ({} out of {})", &movie.display(), movie_index + 1, movie_set.len());

            let parsed_xml: MovieDocument = quick_xml::de::from_str(&String::from_utf8(fs::read(movie).unwrap()).unwrap()).unwrap();
            let movie = parsed_xml.movie;

            if let Some(banner_url) = movie.banner_url {
                fetch_resource(&client, "banner", &banner_url).await?;
            }
            if let Some(thumbnail_url) = movie.thumbnail_url {
                fetch_resource(&client, "thumbnail", &thumbnail_url).await?;
            }
            for rating_icon in icons_from_rating_info(movie.rating_info) {
                fetch_resource(&client, "rating icon", &rating_icon.url).await?;
            }
            // TODO: urls, alternate_rating_image_url

            if fetch_args.fetch_videos {
                for file in movie.files.file {
                    fetch_movie_file(&client, &file).await?;
                }
            }
        }
    }

    Ok(())
}

fn convert_moflex(args: &Args) {
    let mut movies_2d = HashSet::new();
    let mut movies_3d = HashSet::new();

    for region in &args.regions {
        let dir_entries = std::fs::read_dir(format!("samurai/{}", region)).into_iter().flatten().flatten();

        for subdir in dir_entries.filter(|f| f.file_type().unwrap().is_dir()) {
            println!("Gathering video metadata for region {} / language {}", region, subdir.file_name().to_str().unwrap());

            let contained_files_iter = |path| {
                std::fs::read_dir(path)
                        .into_iter()
                        .flatten()
                        .flatten()
                        .filter(|f| f.file_type().unwrap().is_file())
            };

            if args.directory_id.is_some() {
                println!("Cannot constrain media conversion by directory id. Use --title or --movie instead.");
                std::process::exit(1);
            }

            let constrained_fetch = args.title_id.is_some() || args.movie_id.is_some();
            let build_contents_list = |content_name, exclude_list: Vec<_>| {
                Vec::<_>::from_iter(
                    contained_files_iter(subdir.path().join(content_name))
                    .filter(|d| !constrained_fetch || exclude_list.iter().any(|item| *item == d.file_name().to_string_lossy().to_string()))
                    .map(|d| d.path())
                )
            };

            let title_set = Vec::<_>::from_iter(args.title_id.clone().into_iter());
            let movie_set = Vec::<_>::from_iter(args.movie_id.clone().into_iter());

            let mut title_set = build_contents_list("title", title_set);
            title_set.sort_unstable();
            title_set.dedup();
            for title in title_set.iter() {
                let parsed_xml: TitleDocument = quick_xml::de::from_str(&String::from_utf8(fs::read(title).unwrap()).unwrap()).unwrap();
                let title = parsed_xml.title;
                for movie in title.movies.map(|c| c.movie).unwrap_or_default() {
                    for file in movie.files.file {
                        if file.dimension == "3d" {
                            movies_3d.insert(file.movie_url);
                        } else {
                            movies_2d.insert(file.movie_url);
                        }
                    }
                }
            }

            let mut movie_set = build_contents_list("movie", movie_set);
            movie_set.sort_unstable();
            movie_set.dedup();
            for movie in movie_set.iter() {
                let parsed_xml: MovieDocument = quick_xml::de::from_str(&String::from_utf8(fs::read(movie).unwrap()).unwrap()).unwrap();
                let movie = parsed_xml.movie;
                for file in movie.files.file {
                    if file.dimension == "3d" {
                        movies_3d.insert(file.movie_url);
                    } else if file.dimension == "2d" {
                        movies_2d.insert(file.movie_url);
                    } else {
                        panic!("Unknown movie dimension {}", file.dimension);
                    }
                }
            }
        }
    }

    assert!(movies_3d.intersection(&movies_2d).collect::<Vec<_>>().is_empty(), "Video referenced both as 2D and 3D");

    let mut all_videos = movies_2d.iter().collect::<Vec<_>>();
    all_videos.append(&mut (movies_3d.iter().collect::<Vec<_>>()));
    all_videos.sort_unstable();

    if let SubCommand::ConvertMedia(ConvertMediaArgs { filename: Some(filename) }) = &args.command {
        if !filename.starts_with("kanzashi-movie/") {
            println!("File path must start with kanzashi-movie (given filename: {})", filename);
            std::process::exit(1);
        }

        let url = all_videos.iter().find(|v| movie_url_to_filename(v) == *filename);
        if url.is_none() {
            // Can't determine if it's a 3D video or not in this case
            println!("No video metadata found for file {} in the given regions", filename);
            std::process::exit(1);
        }

        all_videos = vec![url.unwrap()];
    }

    if all_videos.is_empty() {
        println!("No video metadata found for the given regions");
        std::process::exit(1);
    }

    for (index, url) in all_videos.iter().enumerate() {
        let moflex = std::path::Path::new(&movie_url_to_filename(*url)).to_owned();
        let filename = moflex.file_name().unwrap();
        let filename = filename.to_string_lossy();

        println!("Converting {} to MP4 ({} out of {})...", filename, index + 1, all_videos.len());

        let is_3d = movies_3d.contains(*url);
        assert!(is_3d || movies_2d.contains(*url));

        // Skip conversion if an MP4 with non-zero size already exists on disk
        // NOTE: This may skip over partial files from a previously cancelled run.
        //       There's no simple way to reliably detect these, so the responsibility is on the user here
        let mp4_filename = moflex.with_extension("mp4");
        if let Ok(metadata) = std::fs::metadata(&mp4_filename) {
            if metadata.len() > 0 {
                println!("    ... MP4 already exists on disk ({} MiB), skipping", metadata.len() / 1024 / 1024);
                continue;
            }
        }

        let out = std::process::Command::new("ffmpeg")
                    .arg("-y") // Overwrite if destination exists
                    .args(["-i", &moflex.to_string_lossy()])
                    // Convert alternating frame 3D to side-by-side 3D.
                    // See https://ffmpeg.org/ffmpeg-filters.html#stereo3d for other options
                    .args(if is_3d { vec!["-vf", "stereo3d=al:sbsl"] } else { vec![] })
                    .arg(mp4_filename)
                    .output();
        match out {
            Err(e) => match e.kind() {
                std::io::ErrorKind::NotFound => panic!("  ERROR: FFmpeg is not installed"),
                _ => panic!("Unknown error while calling ffmpeg")
            },
            Ok(out) => if !out.status.success() {
                println!("  ERROR:");
                std::io::stderr().write_all(&out.stderr).unwrap();
                std::process::exit(1);
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = Args::parse();

    if args.regions.is_empty() {
        use clap::CommandFactory;
        let mut cmd = Args::command();
        cmd.error(clap::ErrorKind::MissingRequiredArgument, "The required argument --regions was not provided").exit();
    }

    let ssl_id = match args.command {
        SubCommand::FetchMetadata(ref args)
        | SubCommand::FetchAll(FetchAllArgs { metadata: ref args, media: _ }) => match args.cert {
            Some(ref cert) => {
                let cert_bytes = fs::read(cert)?;
                Some(reqwest::Identity::from_pem(&cert_bytes)?)
            },
            None => {
                if !args.omit_ninja_contents {
                    println!("3DS client certificate required to download data from Ninja servers.");
                    println!("Specify its location with --cert, or use --omit-ninja-contents to skip this data.");
                    println!("See Readme for details.");
                    std::process::exit(1);
                }
                None
            },
        }
        _ => None
    };

    RESOURCE_CACHE.get_or_init(|| Mutex::new(HashMap::new()));

    {
        let cache = fs::read_to_string("http_log");
        if let Ok(cache) = cache {
            let mut resource_cache = RESOURCE_CACHE.get().unwrap().lock().unwrap();
            for entry in cache.split_terminator(HTTP_HEADERS_SEPARATOR) {
                let entry: serde_json::Value = serde_json::from_str(&entry)?;
                let num_bytes: u64 = match entry["response_headers"]["content-length"].as_str() {
                    Some(num_bytes) => num_bytes.parse().unwrap(),
                    None => 1 // Dummy size returned for transfer-encoding=chunked
                };

                resource_cache.insert(entry["url"].as_str().unwrap().to_string(), num_bytes);
            }
        }
    }
    HTTP_HEADERS_LOG.get_or_init(|| Mutex::new(std::fs::OpenOptions::new().create(true).append(true).open("http_log").unwrap()));

    // Check if we should prompt for --fetch-all-videos to be added
    match args.command {
        SubCommand::FetchMedia(ref mut fetch_args)
        | SubCommand::FetchAll(FetchAllArgs { metadata: _, media: ref mut fetch_args }) => {
            fetch_args.fetch_videos |= fetch_args.fetch_all_videos;

            let constrained_fetch = args.directory_id.is_some() || args.title_id.is_some() || args.movie_id.is_some();

            // TODO: Move the arguments here into a mode-specific subargs struct
            if fetch_args.fetch_videos && !fetch_args.fetch_all_videos && !constrained_fetch {
                println!("\nUsed --fetch-videos without constraint.");
                println!("Do you *really* you want to download *ALL* videos from the eShop servers?");
                println!("Use --title/--movie/--directory to restrict what contents to download videos for, or use --fetch-all-videos if you really need everything.");
                std::process::exit(1);
            }

            // 🤔
            if fetch_args.fetch_all_videos {
                println!("Okay, you're serious about creating a *FULL* archive of the eShop.");
                println!("Please stop to think about the server load this will put on the eShop servers, and reconsider if you *REALLY* need this.");
                println!("To continue, remove this warning from the source code and recompile saveShop.");
                std::process::exit(1);
            }
        },
        _ => {}
    }

    let mut client_builder = reqwest::Client::builder()
                            // Required to access eShop servers without a root CA
                            .danger_accept_invalid_certs(true)
                            // Required for SSL cert to be used
                            .use_rustls_tls();
    if ssl_id.is_some() {
        client_builder = client_builder.identity(ssl_id.unwrap());
    }
    let client = client_builder.build()?;

    fs::create_dir_all(format!("kanzashi")).unwrap();
    fs::create_dir_all(format!("kanzashi-movie")).unwrap();

    for region in &args.regions {
        if !matches!(args.command, SubCommand::FetchMetadata(_)) && !matches!(args.command, SubCommand::FetchAll(_)) {
            break;
        }

        println!("\nProcessing region {}", region);
        fs::create_dir_all(format!("samurai/{}", region)).unwrap();

        // Fetch list of languages first
        let languages: Vec<_> = {
            let data = get_with_retry(&client, format!("{}/{}?shop_id={}", samurai_baseurl(&region), EndPoint::Languages, shop_id)).await?;
            let mut file = File::create(format!("samurai/{}/languages", region)).unwrap();
            write!(file, "{}", data)?;

            let parsed_xml: LanguagesDocument = quick_xml::de::from_str(&data).unwrap();

            if parsed_xml.languages.language.is_empty() {
                println!("Could not find any supported languages for region {}", &region);
                std::process::exit(1);
            }

            println!("Supported languages:");
            for NodeLanguage { name, iso_code } in &parsed_xml.languages.language {
                println!("  {} ({})", iso_code, name);
            }

            parsed_xml.languages.language.into_iter().map(|lang| lang.iso_code).collect()
        };

        // Fetch content metadata
        match args.command {
            SubCommand::FetchMetadata(ref metadata_args)
            | SubCommand::FetchAll(FetchAllArgs { metadata: ref metadata_args, media: _ }) => {
                for language in languages {
                    println!("Fetching metadata for language \"{}\" of region {}", language, region);
                    let locale = Locale { region: region.to_string(), language: language.to_owned() };
                    fs::create_dir_all(format!("samurai/{}/{}/publishers_", locale.region, locale.language)).unwrap();

                    fetch_metadata(&client, &locale, &args, &metadata_args).await?;
                }
            },
            _ => {},
        };
    }

    // Fetch media
    match args.command {
        SubCommand::FetchMedia(ref fetch_args)
        | SubCommand::FetchAll(FetchAllArgs { metadata: _, media: ref fetch_args }) => {
            for region in &args.regions {
                fetch_media_resources(&client, &region, &args, &fetch_args).await?;
            }
        }
        _ => {},
    }

    if matches!(args.command, SubCommand::ConvertMedia(_)) {
        convert_moflex(&args);
    }

    Ok(())
}
