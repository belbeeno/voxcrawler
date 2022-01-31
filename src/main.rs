use chrono::{Date, Utc, TimeZone};
use mysql::*;
use mysql::prelude::*;
use regex::Regex;
use std::{env, str, thread, time};
use std::collections::HashSet;

mod vox_utils;
pub use crate::vox_utils::filters;
pub use crate::vox_utils::validators;

const DB_PATH: &str = "vox.belbeeno.com/voxsearch";
fn get_db_path() -> String { 
    let username = env::var("VOXCRAWLER_USER").unwrap();
    let password = env::var("VOXCRAWLER_PASS").unwrap();
    format!("mysql://{username}:{password}@{DB_PATH}") 
}

struct Listing {
    id: String,
    date: String,
}
impl Listing {
    fn to_string(&self) -> String { format!("File:[{}] Date:[{}]", self.id, self.date) }
}

struct VoxEntry {
    id: u64,
    author: String,
    log_id: String,
    date: String,
    content: String
}
/*
impl VoxEntry {
    fn to_string(&self) -> String { format!("ID:[{}] Content:[{}]", self.id, self.content) }
}
*/

struct VoxIndexData {
    id: u64,
    indexed_content: String,
    has_song: bool,
    has_morshu: bool,
}
/*
impl VoxIndexData {
    fn to_string(&self) -> String { format!("ID:[{}] SONG:[{}] MORSHU:[{}] CONTENT:[{}]", self.id, self.has_song, self.has_morshu, self.indexed_content) }
}
*/

// MArio is missing
// SELECT * FROM `voxes` WHERE `id` = (SELECT `id` FROM `vox_meta` WHERE MATCH(`indexed_content`) AGAINST("mario"));

fn main()
{
    index_log("2021-02-07-voxLog.txt");
}

fn index_log(log_id:&str) {
    let dbpath : String = get_db_path();
    let opts = Opts::from_url(&dbpath).unwrap();
    let pool = Pool::new(opts).unwrap();
    let mut conn = pool.get_conn().unwrap();
    let query = format!("SELECT `id`, `content` FROM `voxes` WHERE `log_id` = \"{log_id}\"");
    let voxes = conn.query_map(query,
        |(new_id, new_content)| {
            VoxEntry { 
                id: new_id,
                author: String::new(),
                log_id: String::new(),
                date: String::new(),
                content:new_content,
            }
        },).unwrap();

    let mut vox_index_data : Vec<VoxIndexData> = Vec::new();
    // Get the db access sorted before we start looping
    let opts = Opts::from_url(&get_db_path()).unwrap();
    let pool = Pool::new(opts).unwrap();
    let mut conn = pool.get_conn().unwrap();
    for vox in voxes {
        //println!("-- VoxEntry -- {}", vox.to_string());
        let has_song :bool = vox.content.contains("^s");
        let has_morshu :bool = vox.content.contains("^m");

        // Perform filtering
        let cleaned_vox = filters::contractions(
                            filters::control_codes(
                            filters::pitch(
                            filters::pause(
                            filters::trunc( vox.content )))));
        let content_arr : Vec<&str> = cleaned_vox.split(' ').collect();
        let mut indexed_content = String::new();
        let mut used_words = HashSet::new();
        for word in content_arr {
            let trimmed = word.trim();

            if trimmed.len() > 0 && !used_words.contains(trimmed) {
                if validators::valid(&trimmed) {
                    used_words.insert(trimmed);
                    indexed_content.push_str(&(format!("{trimmed} ")));
                }
                else {
                    println!("Vox entry [{}] has word {} that is not in the vocab.  Dropping...", vox.id, trimmed);
                }
            }
        }

        vox_index_data.push(VoxIndexData { 
            id: vox.id,
            indexed_content,
            has_song,
            has_morshu,
        });
        //println!("---- Indexed Data -- {}", data.to_string());

        conn.exec_batch(
            r"REPLACE INTO vox_meta (id, indexed_content, has_song, has_morshu)
            VALUES (:author, :log_id, :date, :content)",
            vox_index_data.iter().map(|p| params!{
                "author" => p.id,
                "log_id" => p.indexed_content.clone(),
                "date" => p.has_song,
                "content" => p.has_morshu,
             })).unwrap();
    }

}

#[allow(dead_code)]
fn collect_and_commit() {
    let mut listings : Vec<Listing>= Vec::new();
    let root_req = reqwest::blocking::get("https://rook.zone/voxlogs").unwrap();
    let root_body = root_req.text().unwrap();

    // Get all the entries from the root listing page
    let rx_listings = Regex::new(r#"<a href="([0-9]{4}-[0-9]{2}-[0-9]{2}-.*\.txt)">"#).unwrap();
    for listing_cap in rx_listings.captures_iter(&root_body) {
        //println!("{}", current_name);
        let parsed_data : Date<Utc> = parse_date_from_filename(listing_cap[1].to_string());
        let listing = Listing {
            id: listing_cap[1].to_string(),
            date: parsed_data.format("%Y-%m-%d").to_string(),
        };
        listings.push(listing);
    }

    // Get the db access sorted before we start looping
    let opts = Opts::from_url(&get_db_path()).unwrap();
    let pool = Pool::new(opts).unwrap();
    let mut conn = pool.get_conn().unwrap();

    // Get the voxes for each listing (as identified inside the hrefs above)
    let wait = time::Duration::from_secs(3);
    let rx_voxes = Regex::new(r#"From (\w*):.*\n(.*)"#).unwrap();
    let listing = &listings[0];
    //for listing in listings
    {
        let listing_path = format!("https://rook.zone/voxlogs/{}", listing.id);
        let listing_req = reqwest::blocking::get(listing_path).unwrap();
        let listing_body = listing_req.text().unwrap();
        println!("{}", listing.to_string());

        // Parse all the voxes and their authors in this listing
        let mut voxes : Vec<VoxEntry> = Vec::new();
        for vox_cap in rx_voxes.captures_iter(&listing_body) {
            voxes.push( VoxEntry{
                id: 0,  // Not assigned on submission, it's auto incremented
                author: vox_cap[1].to_string(),
                log_id: listing.id.clone(),
                date: listing.date.clone(),
                content: vox_cap[2].to_string(),
            });
        }
        conn.exec_batch(
            r"INSERT INTO voxes (author, log_id, date, content)
            VALUES (:author, :log_id, :date, :content)",
            voxes.iter().map(|p| params!{
                "author" => p.author.clone(),
                "log_id" => p.log_id.clone(),
                "date" => p.date.clone(),
                "content" => p.content.clone()
             })).unwrap();

        // We have the voxes for this listing!  Now time to feed it to the DB I guess?  Should we have confirmed with the DB earlier if we even needed to do this?
        thread::sleep(wait);
    }
}

fn parse_date_from_filename(name:String) -> chrono::Date<Utc> {
    let split_name :Vec<&str> = name.split("-").collect();
    let year = split_name[0].parse().unwrap();
    let month = split_name[1].parse().unwrap();
    let day = split_name[2].parse().unwrap();

    return chrono::Utc.ymd(year, month, day);
}
