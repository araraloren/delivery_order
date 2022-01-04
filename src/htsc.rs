
use async_std::channel::Sender;
use async_std::fs::File;
use async_std::io::{prelude::BufReadExt, BufReader};
use async_std::sync::Arc;
use encoding_rs::GBK;

use crate::DeliveryOrder;

const HTSC_DATE: usize = 0;
const HTSC_CODE: usize = 2;
const HTSC_NAME: usize = 3;
const HTSC_KIND: usize = 4;
const HTSC_COUNT: usize = 5;
const HTSC_PRIZE: usize = 6;
const HTSC_ACTUAL: usize = 11;
const HTSC_OWNED: usize = 18;

static HTSC_CHECK_TITLE: [&'static str; 20] = [
    "发生日期",
    "备注",
    "证券代码",
    "证券名称",
    "买卖标志",
    "成交数量",
    "成交价格",
    "成交金额",
    "佣金",
    "印花税",
    "过户费",
    "发生金额",
    "剩余金额",
    "申报序号",
    "股东代码",
    "席位代码",
    "委托编号",
    "成交编号",
    "证券数量",
    "其他费",
];

fn create_delivery_order(line: String) -> DeliveryOrder {
    let columns: Vec<&str> = line.trim().split("\t").collect();

    assert_eq!(columns.len(), 20);

    DeliveryOrder::default()
        .with_date(columns[HTSC_DATE].to_owned())
        .with_code(columns[HTSC_CODE].to_owned())
        .with_name(columns[HTSC_NAME].to_owned())
        .with_kind(columns[HTSC_KIND].to_owned())
        .with_count(columns[HTSC_COUNT].to_owned())
        .with_prize(columns[HTSC_PRIZE].to_owned())
        .with_actual(columns[HTSC_ACTUAL].to_owned())
        .with_owned(columns[HTSC_OWNED].to_owned())
}

pub async fn read_from_export_file(
    path: String,
    sender: Arc<Sender<Option<DeliveryOrder>>>,
) -> std::io::Result<()> {
    if path.ends_with("txt") {
        read_from_text_from(path, sender).await?;
    }
    else {
        panic!("Not support current file: {}", path);
    }
    Ok(())
}

async fn read_from_text_from(path: String, sender: Arc<Sender<Option<DeliveryOrder>>>) -> std::io::Result<()> {
    let mut reader = BufReader::new(File::open(&path).await?);
    let mut buffer = Vec::with_capacity(4096);
    let gbk_encoder = GBK;

    if reader.read_until(0x0a as u8, &mut buffer).await? > 0 {
        let (line, _, _) = gbk_encoder.decode(&buffer);
        let title: Vec<String> = line.trim().split("\t").map(|v| String::from(v)).collect();
        // checking the line name
        for i in 0..HTSC_CHECK_TITLE.len() {
            assert_eq!(HTSC_CHECK_TITLE[i], title[i]);
        }
    }

    println!("start reading thread for {}", &path);

    loop {
        buffer.clear();

        let size = reader.read_until(0x0a as u8, &mut buffer).await?;

        if size > 0 {
            let (line, _, _) = gbk_encoder.decode(&buffer);
            let order = create_delivery_order(line.to_string());

            sender
                .send(Some(order))
                .await
                .expect(&format!("Can't send data from read thread: {}!", &path));
        } else {
            sender
                .send(None)
                .await
                .expect(&format!("Can't send data to write thread"));
            println!("reading thread is end {}", &path);
            break;
        }
    }

    Ok(())
}