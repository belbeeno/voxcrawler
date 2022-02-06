use chrono::{Date, Utc, TimeZone};
use mysql::*;
use mysql::prelude::*;
use regex::Regex;
use std::{env, io, str};
use std::collections::HashSet;
use std::time::Instant;

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
impl VoxEntry {
    //fn to_string(&self) -> String { format!("ID:[{}] Content:[{}]", self.id, self.content) }
}

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

fn main() -> io::Result<()> {
    println!("\nWelcome to the vox crawler console!");
    fn print_commands() {
        println!("-- Commands --");
        println!(" n - pull new voxes into the DB, and index them");
        println!(" f YYYY-MM-DD-voxlog.txt - force pull existing log and index it");
        println!(" q - quit");
    }

    let mut input = String::new();
    while input != "q" {
        print_commands();
        io::stdin().read_line(&mut input).unwrap();
        let mut params_iter = input.split_whitespace();
        let command = match params_iter.next() {
            None => "",
            Some(_cmd) => _cmd,
        };
        if command == "n" {
            // pull new voxes
            println!("Retreiving vox listing...");
            let total_now = Instant::now();
            let now  = Instant::now();
            let listings = get_vox_listing();
            let opts = Opts::from_url(&get_db_path()).unwrap();
            let pool = Pool::new(opts).unwrap();
            let mut conn = pool.get_conn().unwrap();
            println!("Listing retrieved in [{}ms], processing...", now.elapsed().as_millis());
            for i in 0..3 {
                if is_on_file(&listings[i].id, &mut conn) {
                    println!("Entry [{}] already on db.  Ignoring...", &listings[i].id);
                }
                else {
                    println!("Retreiving entry [{}]...", listings[i].id);
                    let now = Instant::now();
                    collect_and_commit(&listings[i], &mut conn);
                    println!("Entry retrieved in [{}ms], indexing...", now.elapsed().as_millis());
                    let now = Instant::now();
                    index_log(&listings[i].id, &mut conn);
                    println!("Indexing for entry [{}] complete in [{}ms]", listings[i].id, now.elapsed().as_millis());
                }
            }

            println!("Pull complete!  Total time: [{}s]", total_now.elapsed().as_secs());
        }
        else if command == "f" {
            // force pull existing log
            let opts = Opts::from_url(&get_db_path()).unwrap();
            let pool = Pool::new(opts).unwrap();
            let mut conn = pool.get_conn().unwrap();
            let log_id = params_iter.next().unwrap();
            println!("Force syncing entry for {log_id}");
            let now = Instant::now();
            index_log(log_id, &mut conn);
            println!("Force update complete in [{}s]!", now.elapsed().as_millis());
        }
        else if command == "q" {
            println!("Bye bye!");
            return Ok(());
        }
        else {
            println!("\nUnhandled command \"{input}\"");
        }
        input = String::new();
    }

    //index_log("2021-02-07-voxLog.txt");

    Ok(())
}

fn is_on_file(log_id:&str, conn:&mut PooledConn) -> bool {
    let query = format!("SELECT COUNT(*) FROM `voxes` WHERE `log_id` = \"{log_id}\"");
    let result:Option<u32> = conn.query_first(query).unwrap();
    match result {
        Some(x) => x > 0,
        None => false,
    }
}

fn get_vox_listing() -> Vec<Listing> {
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
    listings
}

fn index_log(log_id:&str, conn:&mut PooledConn) {
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
                            filters::trunc( vox.content.to_lowercase() )))));
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
                    println!("-- Vox entry [{}] has word [{}] that is not in the vocab.  Dropping...", vox.id, trimmed);
                }
            }
        }

        vox_index_data.push(VoxIndexData { 
            id: vox.id,
            indexed_content,
            has_song,
            has_morshu,
        });
    }

    println!("Index data for [{log_id}] compiled, sending to server...");
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

fn collect_and_commit(listing:&Listing, conn:&mut PooledConn) {
    // Get the voxes for each listing (as identified inside the hrefs above)
    let rx_voxes = Regex::new(r#"From (\w*):.*\n(.*)"#).unwrap();
    let listing_path = format!("https://rook.zone/voxlogs/{}", listing.id);
    let listing_req = reqwest::blocking::get(listing_path).unwrap();
    let listing_body = listing_req.text().unwrap();
    println!("{}", listing.to_string());

    // Parse all the voxes and their authors in this listing
    let mut voxes : Vec<VoxEntry> = Vec::new();
    for vox_cap in rx_voxes.captures_iter(&listing_body) {
        voxes.push( VoxEntry{
            id: 0,  // Not assigned on submission, it's auto incremented
            author: filters::sanatize(vox_cap[1].to_string()),
            log_id: listing.id.clone(),
            date: listing.date.clone(),
            content: filters::sanatize(vox_cap[2].to_string()),
        });
    }

    println!("Voxes collected, submitting to db...");
    conn.exec_batch(
        r"INSERT INTO voxes (author, log_id, date, content)
        VALUES (:author, :log_id, :date, :content)",
        voxes.iter().map(|p| params!{
            "author" => p.author.clone(),
            "log_id" => p.log_id.clone(),
            "date" => p.date.clone(),
            "content" => p.content.clone()
         })).unwrap();
}

fn parse_date_from_filename(name:String) -> chrono::Date<Utc> {
    let split_name :Vec<&str> = name.split("-").collect();
    let year = split_name[0].parse().unwrap();
    let month = split_name[1].parse().unwrap();
    let day = split_name[2].parse().unwrap();

    return chrono::Utc.ymd(year, month, day);
}
