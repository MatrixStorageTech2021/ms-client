use std::time::{SystemTime, UNIX_EPOCH};
use std::future::Future;
use std::path::Path;
use std::io::Read;
use std::pin::Pin;

use data_encoding::HEXLOWER;
use futures_util::{Stream, StreamExt};
use ring::digest;
use rand::Rng;
use serde::Serialize;
use tokio::io::AsyncWriteExt;
use bytes::BytesMut;
use indicatif::ProgressBar;

#[derive(Debug, Clone)]
pub struct Client {
    host: Option<String>,
    path: String,
    appid: Option<String>,
    appkey: Option<String>,
}

#[derive(Debug)]
pub struct UploadRet {
    pub code: u64,
    pub size: u64,
    pub elapsed: u64,
    pub sliced: bool,
}

#[derive(Debug)]
pub struct FileStat {
    pub bucket_name: String,
    pub file_name: String,
    pub file_ext: Option<String>,
    pub cid: Option<String>,
    pub file_size: u64,
    pub store_host: String,
    pub valid_until: u64,
}

#[derive(Debug, Clone)]
//TODO: add messages from server as well
struct ServerError(u64);

impl std::fmt::Display for ServerError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Server Error (code={})", self.0)
    }
}

impl std::error::Error for ServerError {}

use pin_project_lite::pin_project;
pin_project! {
    struct AsyncReadProgress<T,E,S: Stream<Item=Result<T,E>>>
    {
        #[pin]
        inner: S,
        total: u64,
        progress: u64,
        name: String,
        pb: ProgressBar,
    }
}

impl<E,S> Stream for AsyncReadProgress<BytesMut,E,S>
where
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
    S: Stream<Item=Result<BytesMut,E>>
{
    type Item = Result<BytesMut, E>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut core::task::Context<'_>,
    ) -> core::task::Poll<Option<Self::Item>> {
        let this = self.project();
        let p : &mut u64 = this.progress;
        let n : &str = this.name;
        let l : &u64 = this.total;
        let pb = this.pb;
        this.inner.poll_next(cx).map_ok(move |b|
            {
                *p += b.len() as u64;
                if *p < *l {
                    pb.set_message(format!("{}", n));
                    pb.set_position(*p);
                } else {
                    pb.finish();
                }
                b
            })
    }
}

impl Client {
    pub fn new(host: &str, path: &str, appid: &str, secret: &str) -> Client {
        Client {
            host: match host {
                "" => None,
                _ => Some(String::from(host)),
            },
            path: String::from(path),
            appid: match appid {
                "" => None,
                _ => Some(String::from(appid)),
            },
            appkey: match secret {
                "" => None,
                _ => Some(String::from(secret)),
            },
        }
    }
}

#[derive(Serialize, Debug)]
struct PartsInfo {
    part_number: u64,
    etag: String,
}

impl Client {
    fn list<T: Serialize + ?Sized>(&self, form:&T, endpoint: &str)
    -> impl Future<Output = Result<reqwest::Response, reqwest::Error>> {
        let headers = BaseHeader::build(self.appid.as_ref().unwrap(),
                                        self.appkey.as_ref().unwrap());

        reqwest::Client::new()
            .post(format!("{}{}{}", self.host.as_ref().unwrap(),
                        &self.path, endpoint))
            .headers(headers)
            .form(&form)
            .send()
    }

    pub async fn list_bucket(&self, page_index: u32, page_size: u32)
    -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let form = [("page_index", page_index.to_string()),
                  ("page_size", page_size.to_string())];
        let resp: serde_json::Value = self.list(&form, "/buckets_list")
            .await?
            .json()
            .await?;
        let code = resp["code"].as_u64().unwrap();
        if  code == 0 {
            let list = resp["data"]["objs"].as_array().unwrap().iter().map(
                |item| {String::from(item["bucket_name"].as_str().unwrap())} ).collect();
            Ok(list)
        }
        else {
            log::debug!("{:#?}", resp);
            Err(ServerError(code).into())
        }
    }

    pub async fn list_file(&self, cid: Option<&str>, bucket_name: Option<&str>, page_index: u32, page_size: u32)
    -> Result<Vec<FileStat>, Box<dyn std::error::Error>> {
        let resp: serde_json::Value = if let Some(cid) = cid {
             let form = [("cid", cid),
                  ("bucket_name", bucket_name.unwrap_or("default"))];
            self.list(&form, "/file_detail")
            .await?
            .json()
            .await?
        } else {
            if let Some(bucket_name) = bucket_name {
                let form = [("page_index", page_index.to_string()),
                            ("page_size", page_size.to_string()),
                            ("bucket_name", bucket_name.to_string())];
                self.list(&form, "/bucket_files_list")
                    .await?
                    .json()
                    .await?
            } else {
                let form = [("page_index", page_index.to_string()),
                            ("page_size", page_size.to_string())];
                self.list(&form, "/file_list")
                    .await?
                    .json()
                    .await?
            }
        };
        let code = resp["code"].as_u64().unwrap();
        if  code == 0 {
            let list = resp["data"]["objs"].as_array().unwrap().iter().map(
                |item| {
                    FileStat {
                        bucket_name: String::from(item["bucket_name"].as_str().unwrap()),
                        file_name: String::from(item["file_name"].as_str().unwrap()),
                        file_ext: if item["file_ext"].is_null() {None} else {Some(String::from(item["file_ext"].as_str().unwrap()))},
                        file_size : item["file_size"].as_str().unwrap().parse().unwrap(),
                        store_host: String::from(item["store_host"].as_str().unwrap()),
                        valid_until: item["valid_time"].as_f64().unwrap_or(0.0).floor() as u64,
                        cid: if item["cid"].is_null() {None} else {Some(String::from(item["cid"].as_str().unwrap()))}
                    }
                }).collect();
            Ok(list)
        }
        else {
            log::debug!("{:#?}", resp);
            Err(ServerError(code).into())
        }
    }

    pub async fn upload_file(&self, file_path: &str, bucket_name: &str, pb: ProgressBar)
    -> Result<UploadRet, Box<dyn std::error::Error>> {
        let path = Path::new(file_path);
        let file_path = path.canonicalize().unwrap();
        let file_name = path.file_name().unwrap().to_str().unwrap();
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        let headers = BaseHeader::build(self.appid.as_ref().unwrap(),
                                        self.appkey.as_ref().unwrap());

        // Get credential
        let resp: serde_json::Value = reqwest::Client::new()
            .post(format!("{}{}{}", self.host.as_ref().unwrap(),
                        &self.path, "/ask_for_upload_credential"))
            .headers(headers)
            .form(&[
                  ("file_name", file_name),
                  ("bucket_name", bucket_name),
            ])
            .send()
            .await?
            .json()
            .await?;

        let code = resp["code"].as_u64().unwrap();
        if code != 0 {
            return Err(ServerError(code).into());
        }

        // upload file
        let mut headers = BaseHeader::build(self.appid.as_ref().unwrap(),
                                        self.appkey.as_ref().unwrap());
        headers.insert("credential", resp["data"]["credential"].as_str().unwrap().to_string().parse().unwrap());
        headers.insert("eventid", resp["data"]["event_id"].as_str().unwrap().to_string().parse().unwrap());
        headers.insert("bucketname", bucket_name.parse().unwrap());
        headers.insert("isverified", 0.to_string().parse().unwrap());
        headers.insert("isprivate", 1.to_string().parse().unwrap());
        headers.insert("filename", file_name.parse().unwrap());
        headers.insert("filesize", file_size.to_string().parse().unwrap());

        let file = tokio::fs::File::open(&file_path).await?;
        let file_len = std::fs::metadata(&file_path).unwrap().len();
        pb.set_length(file_len);

        let reader_progress = AsyncReadProgress {
            inner: tokio_util::codec::FramedRead::new(file,tokio_util::codec::BytesCodec::new()),
            total: file_len,
            progress: 0,
            name: String::from(file_name),
            pb,
        };

        let start_time = std::time::Instant::now();
        let ret: serde_json::Value = reqwest::Client::new()
            .post(format!("{}{}{}", resp["data"]["store_host"].as_str().unwrap(),
                        &self.path, "/upload_file"))
            .headers(headers)
            .multipart(reqwest::multipart::Form::new()
                       .part("file",
                             reqwest::multipart::Part::stream_with_length(
                                 reqwest::Body::wrap_stream(reader_progress),
                                 file_len)
                             .file_name(file_name.to_string())
                             )
                       )
            .send()
            .await?
            .json()
            .await?;
        let elapsed = start_time.elapsed().as_secs();

        //TODO: handle server error

        Ok(UploadRet {
            code: ret["code"].as_u64().unwrap(),
            size: file_size,
            sliced: false,
            elapsed,
        })
    }

    pub async fn upload_file_in_parts(&self, file_path: &str, bucket_name: &str, pb: ProgressBar)
    -> Result<UploadRet, Box<dyn std::error::Error>> {
        let path = Path::new(file_path);
        let file_path = path.canonicalize().unwrap();
        let file_name = path.file_stem().unwrap().to_str().unwrap();
        let file_ext = path.extension().unwrap_or(std::ffi::OsStr::new("")).to_str().unwrap();
        let file_size = std::fs::metadata(&file_path).unwrap().len();

        let headers = BaseHeader::build(self.appid.as_ref().unwrap(),
                                        self.appkey.as_ref().unwrap());

        // create file parts
        let resp: serde_json::Value = reqwest::Client::new()
            .post(format!("{}{}{}", self.host.as_ref().unwrap(),
                        &self.path, "/create_fileparts"))
            .headers(headers.clone())
            .form(&[
                  ("file_name", file_name),
                  ("file_size", file_size.to_string().as_str()),
                  ("bucket_name", bucket_name),
                  ("ext_name", file_ext),
            ])
            .send()
            .await?
            .json()
            .await?;

        let code = resp["code"].as_u64().unwrap();
        if code != 0 {
            return Err(ServerError(code).into());
        }

        // upload parts
        const PART_SIZE: u64 = 1024 * 1024 * 64;
        let mut file = std::fs::File::open(&file_path)?;
        let num_parts = file_size / PART_SIZE +
            if file_size % PART_SIZE == 0 {0} else {1};
        let mut file_parts: Vec<PartsInfo> = Vec::with_capacity(num_parts as usize);

        let start_time = std::time::Instant::now();

        pb.set_length(file_size);
        for part_number in 1..=num_parts {
            let mut buffer : Vec<u8> = Vec::with_capacity(PART_SIZE as usize);
            let _n = file.by_ref().take(PART_SIZE).read_to_end(&mut buffer)?;

            let part_ret: serde_json::Value = reqwest::Client::new()
                .post(format!("{}{}{}", resp["data"]["store_host"].as_str().unwrap(),
                &self.path, "/upload_fileparts"))
                .headers(headers.clone())
                .query(&[
                       ("event_id", resp["data"]["event_id"].as_str().unwrap()),
                       ("key", resp["data"]["key"].as_str().unwrap()),
                       ("part_number", part_number.to_string().as_str()),
                ])
                .multipart(reqwest::multipart::Form::new()
                           .part("file",
                                 reqwest::multipart::Part::bytes(buffer)
                                 .file_name(file_name.to_string())
                                )
                          )
                .send()
                .await?
                .json()
                .await?;

            //TODO: handle server error

            file_parts.push(PartsInfo {part_number: part_ret["data"]["part_number"].as_u64().unwrap(),
                             etag: part_ret["data"]["etag"].as_str().unwrap().to_string()});

            pb.set_message(format!("{}[{}/{}]", file_name, part_number, num_parts));
            pb.inc(_n as u64);
        }
        pb.finish();

        // finish parts
        let ret: serde_json::Value = reqwest::Client::new()
                .post(format!("{}{}{}", self.host.as_ref().unwrap(),
                        &self.path, "/complete_fileparts"))
                //.post("http://localhost:3000/")
                .headers(headers.clone())
                .form(&[
                      ("event_id", resp["data"]["event_id"].as_str().unwrap()),
                      ("key", resp["data"]["key"].as_str().unwrap()),
                      ("fileparts", serde_json::to_string(&file_parts).unwrap().as_str()),
                ])
                .send()
                .await?
                .json()
                .await?;

        //TODO: handle server error

        let elapsed = start_time.elapsed().as_secs();

        log::trace!("sent file_parts\n{}", serde_json::to_string(&file_parts).unwrap().as_str());

        Ok(UploadRet {
            code: ret["code"].as_u64().unwrap(),
            size: file_size,
            sliced: true,
            elapsed,
        })
    }

    pub async fn download_file(&self, cid: &str, bucket_name: &str, output_file: &str, pb: ProgressBar)
    -> Result<UploadRet, Box<dyn std::error::Error>> {
        // Get file details
        let stat = self.list_file(Some(cid), Some(bucket_name), 1, 10)
            .await?
            .pop()
            .unwrap();

        // If expired, extract file again
        if stat.valid_until < SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() {
            let headers = BaseHeader::build(self.appid.as_ref().unwrap(),
                                            self.appkey.as_ref().unwrap());
            let resp: serde_json::Value = reqwest::Client::new()
                .post(format!("{}{}{}", self.host.as_ref().unwrap(),
                &self.path, "/extract_file"))
                .headers(headers)
                .form(&[
                      ("cid", cid),
                      ("bucket_name", bucket_name),
                ])
                .send()
                .await?
                .json()
                .await?;

            let code = resp["code"].as_u64().unwrap();
            if code != 0 {
                return Err(ServerError(code).into());
            }
        }

        // download file
        let mut dest_file = if "" != output_file {
            String::from(output_file)
        } else {
            stat.file_name
        };
        if let Some(file_ext) = stat.file_ext {
            dest_file.push_str(".");
            dest_file.push_str(file_ext.as_ref());
        }

        let headers = BaseHeader::build(self.appid.as_ref().unwrap(),
        self.appkey.as_ref().unwrap());

        let start_time = std::time::Instant::now();

        pb.set_length(stat.file_size);
        let mut stream = reqwest::Client::new()
            .get(format!("{}{}{}", stat.store_host,
                         &self.path, "/download_file"))
            .headers(headers)
            .query(&[("cid", cid)])
            .send()
            .await?
            .bytes_stream();

        let mut size: usize = 0;
        if output_file == "/dev/null" {
            while let Some(b) = stream.next().await {
                size += std::io::Write::write(&mut std::io::sink(), b.unwrap().as_ref()).unwrap();

                pb.set_message(format!("{}", output_file));
                pb.set_position(size as u64);
            }
        } else {
            let mut file = tokio::fs::OpenOptions::new()
                .create(true).write(true).open(&dest_file)
                .await?;
            while let Some(b) = stream.next().await {
                size += file.write(b.unwrap().as_ref()).await?;

                pb.set_message(format!("{}", dest_file));
                pb.set_position(size as u64);
            }
        }
        pb.finish();

        let elapsed = start_time.elapsed().as_secs();

        Ok(UploadRet {
            code: 0,
            size: size as u64,
            sliced: false,
            elapsed,
        })
    }
}

struct BaseHeader();

impl BaseHeader {
    fn build(appid: &str, secret: &str) -> reqwest::header::HeaderMap {
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert("from", "openapi".parse().unwrap());
        headers.insert("appversion", "1.0.0".parse().unwrap());
        headers.insert("appid", appid.parse().unwrap());
        headers.insert("signature", BaseHeader::gen_signature(secret).parse().unwrap());
        headers
    }

    fn gen_signature(secret: &str) -> String {
        const SET: &[u8] = b"0123456789";
        let mut rng = rand::thread_rng();
        let nonce: String = (0..10)
            .map(|_| {
                let idx = rng.gen_range(0..SET.len());
                SET[idx] as char
            })
        .collect();

        let ts: String = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs().to_string();

        let mut ctx = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
        let mut items: Vec<&str> = vec![&nonce, &ts, secret];
        items.sort();
        for item in items {
            ctx.update(item.as_bytes());
        }
        let hash = HEXLOWER.encode(ctx.finish().as_ref());

        "signature=".to_owned() + &hash + "&timestamp=" + &ts + "&nonce=" + &nonce
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_sha() {
        let mut ctx = digest::Context::new(&digest::SHA1_FOR_LEGACY_USE_ONLY);
        let mut items: Vec<&str> = vec!["3209466691", "1621320595", "fe16124703d2319c919fec45b9c14d1d"];
        items.sort();
        for item in items {
            ctx.update(item.as_bytes());
        }
        let hash = HEXLOWER.encode(ctx.finish().as_ref());
        assert!(hash.eq("4ffece8e746362708352f29e50acadc85e7a04f4"));
    }
}
