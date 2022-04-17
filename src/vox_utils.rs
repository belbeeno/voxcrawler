use lazy_static::lazy_static;
use regex::Regex;
use std::collections::HashSet;
use std::fs::File;
use std::io::{self, BufRead};

// Filters for strings sent to `vox_meta`
lazy_static! { static ref COMMAND_RX: Regex = Regex::new(r"^!(tc|op) vox ").unwrap(); }
lazy_static! { static ref TRUNC_RX: Regex = Regex::new(r"[><]\.[0-9]+").unwrap(); }
lazy_static! { static ref PAUSE_RX: Regex = Regex::new(r"[,.?!]").unwrap(); }
lazy_static! { static ref PITCH_RX: Regex = Regex::new(r"([0-9+-]{0,20})([a-zA-Z_*']+[a-zA-Z0-9_]*)([0-9+-]{0,20})").unwrap(); }
lazy_static! { static ref CONTROL_CODES_RX: Regex = Regex::new(r"(\^[a-zA-Z0-9=]*)|(\*)").unwrap(); }
lazy_static! { static ref CONTRACTION_RX : Regex = Regex::new(r"(('s)|(n't))").unwrap(); }
lazy_static! { static ref SHORTHAND_DICTIONARY : Vec<(Regex, &'static str)> = vec![
	(Regex::new(r"\bn1\b").unwrap(), "cnote"),
	(Regex::new(r"\bn2\b").unwrap(), "catnote"),
	(Regex::new(r"\bn3\b").unwrap(), "cuicanote"),
	(Regex::new(r"\bn4\b").unwrap(), "dootnote"),
	(Regex::new(r"\bn5\b").unwrap(), "yossynote"),
	(Regex::new(r"\bn6\b").unwrap(), "puhnote"),
	(Regex::new(r"\bn7\b").unwrap(), "bupnote"),
	(Regex::new(r"\bn8\b").unwrap(), "dantnote"),
	(Regex::new(r"\bn9\b").unwrap(), "downote"),
	(Regex::new(r"\bn10\b").unwrap(), "slapnote"),
	(Regex::new(r"\bn11\b").unwrap(), "jarnote"),
	(Regex::new(r"\bn12\b").unwrap(), "orchnote"),
	(Regex::new(r"\bn13\b").unwrap(), "shynote"),
	(Regex::new(r"\bn14\b").unwrap(), "morshunote"),
	(Regex::new(r"\bn15\b").unwrap(), "hazymazenote"),
	(Regex::new(r"\bn16\b").unwrap(), "hauntnote"),
	(Regex::new(r"\bn17\b").unwrap(), "pizzicatonote"),
	(Regex::new(r"\bn18\b").unwrap(), "zunnote"),
	(Regex::new(r"\bn19\b").unwrap(), "banjonote"),
	(Regex::new(r"\bn20\b").unwrap(), "banjonote2"),
	(Regex::new(r"\bn21\b").unwrap(), "banjonote3"),
	(Regex::new(r"\bn22\b").unwrap(), "diddynote"),
	(Regex::new(r"\bn23\b").unwrap(), "diddynote2"),
	(Regex::new(r"\bn24\b").unwrap(), "diddynote3"),
	(Regex::new(r"\bkk1\b").unwrap(), "kk_na"),
	(Regex::new(r"\bkk2\b").unwrap(), "kk_mi"),
	(Regex::new(r"\bkk3\b").unwrap(), "kk_me"),
	(Regex::new(r"\bkk4\b").unwrap(), "kk_o"),
	(Regex::new(r"\bkk5\b").unwrap(), "kk_oh"),
	(Regex::new(r"\bkk6\b").unwrap(), "kk_way"),
	(Regex::new(r"\bkk7\b").unwrap(), "kk_now"),
	(Regex::new(r"\bkk8\b").unwrap(), "kk_whistle"),
	(Regex::new(r"\bkk9\b").unwrap(), "kk_howl"),
	(Regex::new(r"\bkk10\b").unwrap(), "kk_hm"),
	(Regex::new(r"\bkk11\b").unwrap(), "kk_hmlow"),
	(Regex::new(r"\bkk12\b").unwrap(), "kk_snare"),
	(Regex::new(r"\bkk13\b").unwrap(), "kk_snare2"),
	(Regex::new(r"\bkk14\b").unwrap(), "kk_hat"),
	(Regex::new(r"\bd1\b").unwrap(), "sonic_snare"),
	(Regex::new(r"\bd2\b").unwrap(), "sonic_kick"),
	(Regex::new(r"\bd3\b").unwrap(), "sonic_go"),
	(Regex::new(r"\bd4\b").unwrap(), "hazymazedrum"),
	(Regex::new(r"\bd5\b").unwrap(), "hazymazewood"),
	(Regex::new(r"\bd6\b").unwrap(), "yosbongonote"),
	(Regex::new(r"\brn\b").unwrap(), "restnote"),
	]; }
lazy_static! { static ref TOO_SHORT_RX : Regex = Regex::new(r"(^| )([a-zA-Z0-9_']{1,2})($|[\r\n\s ])").unwrap(); }
lazy_static! { static ref CLEANUP_RX : Regex = Regex::new(r"( [ ]+)").unwrap(); }

const VERBOSE : bool = false;

pub mod filters {
	use crate::vox_utils::VERBOSE;

	fn print_if_verbose(step_name:&str, vox:&String) {
		if VERBOSE {
			println!("Step \"{step_name}\": [{vox}]");			
		}

	}

	/////////////////////////////////////////////
	// Filters for strings sent to `voxes`
	pub fn sanatize(vox:String) -> String {
		let output = vox.replace("\"", "").replace("‘", "'").replace("’", "'");
		print_if_verbose("sanatize", &output);
		output
	}

	/////////////////////////////////////////////
	// Filters for strings sent to `vox_meta`
	use crate::vox_utils::COMMAND_RX;
	pub fn commands(vox:String) -> String 
	{
		let output = COMMAND_RX.replace_all(&vox, "").to_string();
		if VERBOSE { print_if_verbose("commands", &output); }
		output
	}

	use crate::vox_utils::TRUNC_RX;
	pub fn trunc(vox:String) -> String 
	{
		let output = TRUNC_RX.replace_all(&vox, "").to_string();
		if VERBOSE { print_if_verbose("trunc", &output); }
		output
	}

	use crate::vox_utils::PAUSE_RX;
	pub fn pause(vox:String) -> String 
	{
		let output = PAUSE_RX.replace_all(&vox, " ").to_string();
		if VERBOSE { print_if_verbose("pause", &output); }
		output
	}

	use crate::vox_utils::PITCH_RX;
	pub fn pitch(vox:String) -> String { 
		let output = PITCH_RX.replace_all(&vox, |caps: &regex::Captures| {caps[2].to_string()}).to_string();
		if VERBOSE { print_if_verbose("pitch", &output); }
		output
	}

	use crate::vox_utils::CONTROL_CODES_RX;
	pub fn control_codes(vox:String) -> String { 
		let output = CONTROL_CODES_RX.replace_all(&vox, "").to_string();
		if VERBOSE { print_if_verbose("control_codes", &output); }
		output
	}

	use crate::vox_utils::CONTRACTION_RX;
	pub fn contractions(vox:String) -> String { 
		// No lookahead with Rust regex... ah well
		let output = CONTRACTION_RX.replace_all(&vox, |caps: &regex::Captures| {format!(" {} ", &caps[1])}).to_string().replace("ca n't", "can't");
		if VERBOSE { print_if_verbose("contractions", &output); }
		output
	}

	use crate::vox_utils::SHORTHAND_DICTIONARY;
	pub fn remap_note_shorthand(vox:String) -> String {
		// I don't have access to InnoDB config to decrease the min token size on Dreamhost, so just default the indexing to use the long form
		let mut ret_val:String = vox.clone();
		for entry in SHORTHAND_DICTIONARY.iter() {
			ret_val = entry.0.replace_all(&ret_val, entry.1).to_string();
		}
		if VERBOSE { print_if_verbose("remap_note_shorthand", &ret_val); }
		ret_val
	}

	use crate::vox_utils::TOO_SHORT_RX;
	pub fn pad_short_words(vox:String) -> String {
		// No lookback so keep doiing it until it's done
		let mut prev_output : String = vox.clone();
		let mut output : String = TOO_SHORT_RX.replace_all(&vox, |caps: &regex::Captures| {format!("{}{:_<3}{}", &caps[1], &caps[2], &caps[3])}).to_string();
		let mut i = 0;
		while prev_output != output {
			prev_output = output;			
			output = TOO_SHORT_RX.replace_all(&prev_output, |caps: &regex::Captures| {format!("{}{:_<3}{}", &caps[1], &caps[2], &caps[3])}).to_string();
			i = i + 1;
			if i > 100 {
				panic!("Timeout on pad_short_words: couldn't conclude on entry {}", vox);
			}
		}
		if VERBOSE { print_if_verbose("pad_short_words", &output); }
		output
	}

	use crate::vox_utils::CLEANUP_RX;
	pub fn cleanup(vox:String) -> String { 
		let output = CLEANUP_RX.replace_all(&vox, " ").to_string();
		if VERBOSE { print_if_verbose("cleanup", &output); }
		output
	}
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