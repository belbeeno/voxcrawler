use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead};

// Filters for strings sent to `vox_meta`
lazy_static! { static ref TRUNC_RX: Regex = Regex::new(r"[><]\.[0-9]+").unwrap(); }
lazy_static! { static ref PAUSE_RX: Regex = Regex::new(r"[,.?!]").unwrap(); }
lazy_static! { static ref PITCH_RX: Regex = Regex::new(r"([0-9+-]{0,20})([a-zA-Z_*']+[a-zA-Z0-9_]*)([0-9+-]{0,20})").unwrap(); }
lazy_static! { static ref CONTROL_CODES_RX: Regex = Regex::new(r"(\^[a-zA-Z0-9=]*)|(\*)").unwrap(); }
lazy_static! { static ref CONTRACTION_RX : Regex = Regex::new(r"(('s)|(n't))").unwrap(); }
lazy_static! { static ref CLEANUP_RX : Regex = Regex::new(r"( [ ]+)").unwrap(); }

pub mod filters {
	/////////////////////////////////////////////
	// Filters for strings sent to `voxes`
	pub fn sanatize(vox:String) -> String {
		vox.replace("\"", "").replace("‘", "'").replace("’", "'")
	}

	/////////////////////////////////////////////
	// Filters for strings sent to `vox_meta`
	use crate::vox_utils::TRUNC_RX;
	pub fn trunc(vox:String) -> String { TRUNC_RX.replace_all(&vox, "").to_string() }

	use crate::vox_utils::PAUSE_RX;
	pub fn pause(vox:String) -> String { PAUSE_RX.replace_all(&vox, " ").to_string() }

	use crate::vox_utils::PITCH_RX;
	pub fn pitch(vox:String) -> String { 
		PITCH_RX.replace_all(&vox, |caps: &regex::Captures| {caps[2].to_string()}).to_string()
	}

	use crate::vox_utils::CONTROL_CODES_RX;
	pub fn control_codes(vox:String) -> String { CONTROL_CODES_RX.replace_all(&vox, "").to_string() }

	use crate::vox_utils::CONTRACTION_RX;
	pub fn contractions(vox:String) -> String { 
		// No lookahead with Rust regex... ah well
		CONTRACTION_RX.replace_all(&vox, |caps: &regex::Captures| {format!(" {} ", &caps[1])}).to_string().replace("ca n't", "can't") 
	}

	use crate::vox_utils::CLEANUP_RX;
	pub fn cleanup(vox:String) -> String { CLEANUP_RX.replace_all(&vox, " ").to_string() }
}

lazy_static! { static ref VOX_DB : HashSet<String> = {
	let mut val = HashSet::new();
	let file = match File::open("vox_db.txt")  {
		Err(e) => panic!("Opening vox_db.txt failed: {:?}", e),
		Ok(file) => file,
	};
	let lines = io::BufReader::new(file).lines();
	for line in lines {
		if let Ok(prim) = line {
			val.insert(prim);
		}
	}
	val
};}

pub mod validators {
	use crate::vox_utils::VOX_DB;
	pub fn valid(word:&str) -> bool { VOX_DB.contains(word) }
}