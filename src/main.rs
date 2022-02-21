use chrono::{Date, Utc, TimeZone};
use mysql::*;
use mysql::prelude::*;
use regex::Regex;
use std::{env, io, str};
use std::collections::HashSet;
use std::fs::{File};
use std::io::Read;
use std::io::Write;
use std::path::Path;
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
    println!("\n=== Welcome to the vox crawler console! ===");
    fn print_commands() {
        println!("== Commands ==");
        println!(" n - pull new voxes into the DB, and index them");
        println!(" m - pull voxes from file into the DB and index them");
        println!(" f YYYY-MM-DD-voxlog.txt - force pull existing log and index it");
        println!(" q - quit");
    }

    let mut input = String::new();
    while !input.starts_with("q") {
        print_commands();
        input = String::new();
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
            for listing in listings{
                if is_on_file(&listing.id, &mut conn) {
                    println!("Entry [{}] already on db.  Ignoring...", &listing.id);
                }
                else {
                    println!("Retreiving entry [{}]...", listing.id);
                    let now = Instant::now();
                    collect_and_commit(&listing, &mut conn);
                    println!("Entry retrieved in [{}ms], indexing...", now.elapsed().as_millis());
                    let mut errs : Vec<(u64, String)> = Vec::new();
                    let now = Instant::now();
                    index_log(&listing.id, &mut conn, &mut errs);
                    println!("Indexing for entry [{}] complete in [{}ms]", listing.id, now.elapsed().as_millis());
                    print_report_to_file(listing.id, errs);
                }
            }

            println!("Pull complete!  Total time: [{}s]", total_now.elapsed().as_secs());
        }
        else if command == "f" {
            // force pull existing log
            let opts = Opts::from_url(&get_db_path()).unwrap();
            let pool = Pool::new(opts).unwrap();
            let mut conn = pool.get_conn().unwrap();

            let mut iter = params_iter.next();
            while iter != None {
                let log_id = iter.unwrap();
                println!("Force syncing entry for {log_id}");
                let now = Instant::now();
                let mut errs : Vec<(u64, String)> = Vec::new();
                index_log(&log_id, &mut conn, &mut errs);
                print_report_to_file(log_id.to_string(), errs);
                println!("Force update complete in [{}s]!", now.elapsed().as_millis());
                iter = params_iter.next();
            }
        }
        else if command == "m" {
            // force pull existing log
            let opts = Opts::from_url(&get_db_path()).unwrap();
            let pool = Pool::new(opts).unwrap();
            let mut conn = pool.get_conn().unwrap();

            let mut iter = params_iter.next();
            while iter != None {
                let file_path = iter.unwrap().to_string();
                let path = Path::new(&file_path);
                println!("Force syncing entry for file {file_path}");
                let now = Instant::now();
                let file_name = path.file_name().unwrap().to_str().unwrap().to_string();
                let parsed_date : Date<Utc> = parse_date_from_filename(file_name);

                let listing = Listing {
                    id: path.file_name().unwrap().to_str().unwrap().to_string(),
                    date: parsed_date.format("%Y-%m-%d").to_string(),
                };
                load_and_commit(&listing, path, &mut conn);
                println!("Entry retrieved in [{}ms], indexing...", now.elapsed().as_millis());
                let mut errs : Vec<(u64, String)> = Vec::new();
                let now = Instant::now();
                match path.file_name() {
                    Some(s) => index_log(s.to_str().unwrap(), &mut conn, &mut errs),
                    None => eprintln!("Path submitted for m has no filename"),
                }
                print_report_to_file(listing.id, errs);
                println!("Force update complete in [{}s]!", now.elapsed().as_millis());
                iter = params_iter.next();
            }
        }
        else if command != "q" {
            println!("\nUnhandled command \"{input}\"");
        }
    }

    println!("Bye bye!");
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

fn index_log(log_id:&str, conn:&mut PooledConn, errs:&mut Vec<(u64, String)>) {
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
        let cleaned_vox = filters::cleanup(
                            filters::contractions(
                            filters::control_codes(
                            filters::pitch(
                            filters::pause(
                            filters::trunc( 
                            filters::sanatize( vox.content.to_lowercase() )))))));
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
                    errs.push((vox.id, trimmed.to_string()));
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

fn commit(listing:&Listing, body:String, conn:&mut PooledConn) {
    // Parse all the voxes and their authors in this listing
    let rx_voxes = Regex::new(r#"From (\w*):.*\n(.*)"#).unwrap();
    let mut voxes : Vec<VoxEntry> = Vec::new();
    for vox_cap in rx_voxes.captures_iter(&body) {
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

fn collect_and_commit(listing:&Listing, conn:&mut PooledConn) {
    // Get the voxes for each listing (as identified inside the hrefs above)
    let listing_path = format!("https://rook.zone/voxlogs/{}", listing.id);
    let listing_req = reqwest::blocking::get(listing_path).unwrap();
    let listing_body = listing_req.text().unwrap();
    println!("{}", listing.to_string());

    commit(listing, listing_body, conn);
}

fn load_and_commit(listing:&Listing, path:&Path, conn:&mut PooledConn) {
    let path_str = match path.to_str() {
        Some(str) => str,
        None => "Undefined",
    };
    let file_result = File::options().read(true).open(path);
    if file_result.is_err() {
        eprintln!("Couldn't load voxes from file [{path_str}] because [{}]", file_result.unwrap_err().to_string());
        return;
    }
    let mut file = file_result.unwrap();
    let mut file_body = String::new();
    match file.read_to_string(&mut file_body) {
        Ok(_size) => {
            println!("{}", listing.to_string());
            commit(listing, file_body, conn)
        },
        Err(e) => eprintln!("Couldn't load voxes from file [{path_str}] because [{}]", e.to_string()),
    }
}

fn parse_date_from_filename(name:String) -> chrono::Date<Utc> {
    println!("{}", name);
    let split_name :Vec<&str> = name.split("-").collect();
    let year = split_name[0].parse().unwrap();
    let month = split_name[1].parse().unwrap();
    let day = split_name[2].parse().unwrap();

    return chrono::Utc.ymd(year, month, day);
}

fn print_report_to_file(log_id:String, errors:Vec<(u64,String)>) {
    let now = Utc::now();
    let filename = format!("logs/VoxReport_{}.txt", now.format("%F"));
    println!("Writing to log [{filename}]...");
    let path = Path::new(&filename);
    let display = path.display();

    let mut file : std::fs::File = match File::options().append(true).create(true).open(&path) {
        Ok(ret) => ret,
        Err(e) => panic!("Could not create report [{}], reason: [{}]", display, e),
    };

    if let Err(e) = writeln!(file, "=== Report for [{}] - Error Count: {} ===", log_id, errors.len()) {
        eprintln!("Couldn't print to file [{display}], reason[{e}]");
        return;
    }
    if errors.len() == 0 {
        if let Err(e) = writeln!(file, "No errors detected!  Great job everyone!") {
            eprintln!("Couldn't print to file [{display}], reason[{e}]");
        }
        return;
    }
    for (id,content) in errors {
        if let Err(e) = writeln!(file, "[{}] - {}", id, content) {
            eprintln!("Couldn't print to file [{display}], reason[{e}]");
            return;
        }
    }
}
