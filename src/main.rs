use anyhow::Result;
use prettytable::{cell, format, row, Table};
use rusoto_core::Region;
use rusoto_s3::{ListObjectsV2Request, S3Client, S3};
use structopt::StructOpt;

#[derive(StructOpt, Debug)]
struct Opt {
    path: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    let opt = Opt::from_args();

    let s3_client = S3Client::new(Region::default());

    if let Some(path) = opt.path {
        let path_parts: Vec<&str> = path.splitn(2, '/').collect();
        let bucket_name = path_parts[0].to_string();
        let prefix = if path_parts.len() > 1 {
            path_parts[1].to_string()
        } else {
            "".to_string()
        };

        get_folders_and_files(&s3_client, &bucket_name, &prefix).await?;
    } else {
        get_buckets(&s3_client).await?;
    }

    Ok(())
}

async fn get_buckets(s3_client: &S3Client) -> Result<()> {
    let response = s3_client.list_buckets().await?;

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    table.set_titles(row!["Type", "Name"]);

    for bucket in response.buckets.unwrap_or_default() {
        table.add_row(row!["Bucket", bucket.name.unwrap()]);
    }

    table.printstd();
    Ok(())
}

async fn get_folders_and_files(
    s3_client: &S3Client,
    bucket_name: &str,
    prefix: &str,
) -> Result<()> {
    let request = ListObjectsV2Request {
        bucket: bucket_name.to_string(),
        delimiter: Some("/".to_string()),
        prefix: if prefix.is_empty() {
            None
        } else {
            Some(prefix.to_string())
        },
        ..Default::default()
    };
    let response = s3_client.list_objects_v2(request).await?;

    let mut table = Table::new();
    table.set_format(*format::consts::FORMAT_NO_BORDER_LINE_SEPARATOR);
    table.set_titles(row!["Type", "Bucket", "Key"]);

    if let Some(common_prefixes) = response.common_prefixes {
        for prefix in common_prefixes {
            table.add_row(row!["Folder", bucket_name, prefix.prefix.unwrap()]);
        }
    }

    if let Some(contents) = response.contents {
        for object in contents {
            if object.key.as_deref() != Some(prefix) {
                table.add_row(row!["File", bucket_name, object.key.unwrap()]);
            }
        }
    }

    table.printstd();
    Ok(())
}
