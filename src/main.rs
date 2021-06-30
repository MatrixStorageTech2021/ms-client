use msclient::Client;
use std::io::Write;
use std::sync::Arc;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};

//TODO: take args instead of hardcode here, or even better, ask server for the info
const APPID: &str = "YOUR_APPID";
const APPKEY: &str = "YOUR_APPKEY";

const HOST: &str = "HOST_ADD";
const PATH: &str = "/store/openapi/v1";

const PB_STYLE: &str = "{msg:15!} [{binary_bytes_per_sec:.green}] [{wide_bar:.cyan}] {bytes}/{total_bytes} ({eta})";
const PB_PROGCHAR: &str = "█░";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = clap::App::new("Matrix Storage Service Client")
        .version("0.1.1")
        .author("xqinx>")
        .about("simple client for Matrix Storage service")
        .args_from_usage(
            "-n, --no               'Dry run'
            -v...                   'Sets the level of verbosity'")
        .subcommand(clap::SubCommand::with_name("list")
                    .about("list file information")
                    .args_from_usage(
                         "-b, --bucket=[name]     'List files under specified bucket, otherwise all buckets'
                         -i, --id=[CID]           'If present, list file with this CID'
                         -p, --page=[num]         'Page index, starting from 1'
                         -n, --count=[num]        'Page size, number of items per page'
                         -a, --all                'List detailed file information'")
                    )
        .subcommand(clap::SubCommand::with_name("upload")
                    .about("upload files")
                    .args_from_usage(
                         "-o, --output=[FILE]    'Write test results to a file'
                         -b, --bucket=[name]     'Target bucket name for upload, or \"default\"'
                         <FILE>...               'Target file(s) for upload'
                         -s, --slice             'Slice and upload file in parts'")
                    )
        .subcommand(clap::SubCommand::with_name("download")
                    .about("download files")
                    .args_from_usage(
                         "-o, --output=[FILE]    'Write test results to a file'
                         -b, --bucket=[name]     'Target bucket name for upload, or \"default\"'
                         -d, --dest=[FILE]       'Destination file, if not provided, information from file will be used'
                         <CID>...                'Target file CID(s) for upload'")
                    )
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        1 => env_logger::Env::default().default_filter_or("info"),
        2 => env_logger::Env::default().default_filter_or("debug"),
        3 => env_logger::Env::default().default_filter_or("trace"),
        0 | _ => env_logger::Env::default().default_filter_or("warn"),
    };

    env_logger::Builder::from_env(log_level)
        .format_timestamp(None)
        .target(env_logger::Target::Stdout)
        .init();

    let m_bar = Arc::new(MultiProgress::new());
    let p = m_bar.add(ProgressBar::hidden());
    let m = Arc::clone(&m_bar);
    let handle_m = tokio::task::spawn_blocking(move || (*m).join().unwrap());

    match matches.subcommand() {
        ("upload",  Some(upload_matches)) => upload(upload_matches, matches.is_present("no"), m_bar).await,
        ("download",  Some(download_matches)) => download(download_matches, matches.is_present("no"), m_bar).await,
        ("list",    Some(list_matches)) => list(list_matches).await,
        _                       => {},
    }

    p.finish_and_clear();
    handle_m.await?;

    Ok(())
}

async fn list(list_matches: &clap::ArgMatches<'static>) {
    let client = Client::new(HOST, PATH, APPID, APPKEY);
    let page_index:u32 = list_matches.value_of("page").unwrap_or("1").parse().unwrap();
    let page_size:u32 = list_matches.value_of("count").unwrap_or("10").parse().unwrap();
    let bucket_name = list_matches.value_of("bucket");
    let cid = list_matches.value_of("id");
    let resp = client.list_file(cid, bucket_name, page_index,page_size).await.unwrap_or_else(|e| panic!("Error: {:#?}", e));
    for item in &resp {
        if list_matches.is_present("all") {
            println!("{:?}", item);
        } else {
            println!("{}", item.cid.as_deref().unwrap_or("None"));
        }
    }
    log::debug!("{:#?}", resp);
}

async fn upload(upload_matches: &clap::ArgMatches<'static>, dry_run: bool, m_bar: Arc<MultiProgress>) {
    let file_to_upload: Vec<String> = upload_matches.values_of("FILE").unwrap().map(|f| f.to_owned()).collect();
    let target_bucket = upload_matches.value_of("bucket").unwrap_or("default").to_owned();
    let output_file = upload_matches.value_of("output");

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);

    if !dry_run {
        log::info!("Uploading file {:?} to bucket {}", &file_to_upload, &target_bucket);

        for file in &file_to_upload {
            let c = Client::new(HOST, PATH, APPID, APPKEY);
            let ttx = tx.clone();
            let f = String::from(file);
            let b = target_bucket.clone();
            let p = m_bar.add(ProgressBar::new(0));
            p.set_style(ProgressStyle::default_bar()
                         .template(PB_STYLE)
                         .progress_chars(PB_PROGCHAR));

            log::info!("Uploading file {}", file);

            if upload_matches.is_present("slice") {
                tokio::spawn(async move {
                    let ret = c.upload_file_in_parts(&f, &b, p).await.unwrap();
                    ttx.send(ret).await.unwrap();
                });
            } else {
                tokio::spawn(async move {
                    let ret = c.upload_file(&f, &b, p).await.unwrap();
                    ttx.send(ret).await.unwrap();
                });
            }
        }
        drop(tx);
    } else {
        log::info!("Dry run: upload file {:?} to bucket {}", &file_to_upload, &target_bucket);
        let tx_c = tx.clone();
        tokio::spawn(async move {
            tx_c.send(msclient::UploadRet {code: 0, size: 1000000, sliced: true, elapsed:100})
                .await.unwrap();
        });
        drop(tx);
    }

    while let Some(res) = rx.recv().await {
        let time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .unwrap().as_secs();
        let size_in_mb = res.size / 1_000_000;
        let speed_in_kbs = res.size / res.elapsed / 1_000;
        log::debug!("{},{},{},{},{}",time, res.code, size_in_mb, speed_in_kbs, res.sliced);
        if let Some(file) = output_file {
            let mut f = std::fs::OpenOptions::new()
                .create(true).append(true).open(file).unwrap();
            writeln!(f, "{},{},{},{},{}",time, res.code, size_in_mb, speed_in_kbs, res.sliced).unwrap();
        }
    }
}


async fn download(download_matches: &clap::ArgMatches<'static>, dry_run: bool, m_bar: Arc<MultiProgress>) {
    let file_to_download: Vec<String> = download_matches.values_of("CID").unwrap().map(|f| f.to_owned()).collect();
    let target_bucket = download_matches.value_of("bucket").unwrap_or("default").to_owned();
    let output_file = download_matches.value_of("output");
    let dest_file = download_matches.value_of("dest").unwrap_or("").to_owned();

    let (tx, mut rx) = tokio::sync::mpsc::channel(10);

    if !dry_run {
        log::info!("downloading file {:?} from bucket {}", &file_to_download, &target_bucket);

        let client = Client::new(HOST, PATH, APPID, APPKEY);
        for file in &file_to_download {
            let ttx = tx.clone();
            let c = client.clone();
            let f = String::from(file);
            let b = target_bucket.clone();
            let d = dest_file.clone();
            let p = m_bar.add(ProgressBar::new(0));
            p.set_style(ProgressStyle::default_bar()
                        .template(PB_STYLE)
                        .progress_chars(PB_PROGCHAR));

            log::info!("downloading file {}", file);
            tokio::spawn(async move {
                let ret = c.download_file(&f, &b, &d, p).await.unwrap();
                ttx.send(ret).await.unwrap();
            });
        }
        drop(tx);
    } else {
        log::info!("Dry run: download file {:?} from bucket {}", &file_to_download, &target_bucket);
        let tx_c = tx.clone();
        tokio::spawn(async move {
            tx_c.send(msclient::UploadRet {code: 0, size: 1000000, sliced: true, elapsed:100})
                .await.unwrap();
        });
        drop(tx);
    }

    while let Some(res) = rx.recv().await {
        let time = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
            .unwrap().as_secs();
        let size_in_mb = res.size / 1_000_000;
        let speed_in_kbs = res.size / res.elapsed / 1_000;
        log::debug!("{},{},{},{},{}",time, res.code, size_in_mb, speed_in_kbs, res.sliced);
        if let Some(file) = output_file {
            let mut f = std::fs::OpenOptions::new()
                .create(true).append(true).open(file).unwrap();
            writeln!(f, "{},{},{},{},{}",time, res.code, size_in_mb, speed_in_kbs, res.sliced).unwrap();
        }
    }
}
