fn main() { println!("DATA: {}, STACK: {}, FSIZE: {}, NPROC: {}", libc::RLIMIT_DATA, libc::RLIMIT_STACK, libc::RLIMIT_FSIZE, libc::RLIMIT_NPROC); }
