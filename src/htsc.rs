use async_std::channel::Sender;
use async_std::fs::File;
use async_std::io::{prelude::BufReadExt, BufReader};
use async_std::sync::{Arc, Mutex};
use encoding_rs::GBK;
use std::borrow::BorrowMut;
use std::collections::HashMap;

use crate::{DeliveryOrder, Trade};

#[derive(Debug)]
pub struct Context {
    count: HashMap<String, i64>,
    debug: bool,
}

impl Context {
    pub fn new() -> Self {
        Self {
            count: HashMap::default(),
            debug: false,
        }
    }

    pub fn has(&self, key: &str) -> bool {
        self.count.contains_key(key)
    }

    pub fn get_count(&self, key: &str) -> Option<&i64> {
        self.count.get(key)
    }

    pub fn set_debug(&mut self, debug: bool) -> &mut Self {
        self.debug = debug;
        self
    }

    pub fn add_count(&mut self, key: String, count: i64) {
        *self.count.entry(key).or_insert(0) += count;
    }

    pub fn gen_title() -> Vec<String> {
        [
            "成交日期",
            "证券代码",
            "证券名称",
            "交易类别",
            "成交数量",
            "成交价格",
            "发生金额",
            "证券余额",
        ]
        .map(|v| v.to_owned())
        .to_vec()
    }

    pub fn gen_order(&mut self, titles: &Vec<String>, line: String) -> DeliveryOrder {
        let columns: Vec<&str> = line.trim().split("\t").collect();

        assert_eq!(columns.len(), titles.len());

        let mut delivery_order = DeliveryOrder::default();
        let mut count = 0;
        let mut left_count = None;

        for (title, &column) in titles.iter().zip(columns.iter()) {
            let value = column.trim().to_owned();

            match title.as_str() {
                "发生日期" | "日期" => {
                    delivery_order = delivery_order.with_date(value);
                }
                "证券代码" => {
                    delivery_order = delivery_order.with_code(value);
                }
                "证券名称" | "股票名称" => {
                    delivery_order = delivery_order.with_name(value);
                }
                "成交数量" | "发生数量" => {
                    count = value
                        .parse::<f64>()
                        .expect(&format!("Can not parse {} as i64", column))
                        as i64;
                    count = count.abs();
                }
                "成交价格" | "成交均价" => {
                    delivery_order = delivery_order.with_prize(value);
                }
                "发生金额" | "收付金额" => {
                    delivery_order = delivery_order.with_amount(value);
                }
                "业务名称" | "业务标志" => {
                    let value = match column {
                        "证券卖出" => {
                            delivery_order = delivery_order.with_trade(crate::Trade::Sell);
                            "卖出"
                        }
                        "证券买入" | "开放基金认购结果" => {
                            delivery_order = delivery_order.with_trade(crate::Trade::Buy);
                            "买入"
                        }
                        "银证转存" | "银行转存" | "利息归本" => {
                            delivery_order = delivery_order.with_trade(crate::Trade::In);
                            "银证转入"
                        }
                        "银证转取" | "银行转取" => {
                            delivery_order = delivery_order.with_trade(crate::Trade::Out);
                            "银证转出"
                        }
                        _ => {
                            delivery_order = delivery_order.with_trade(crate::Trade::Ignore);
                            continue;
                        }
                    };
                    delivery_order = delivery_order.with_kind(value.to_owned());
                }
                "证券数量" => {
                    left_count = Some(value.parse::<f64>().unwrap() as i64);
                }
                _ => {}
            }
        }
        if delivery_order.get_trade() == &Trade::Sell {
            count = -count;
        }
        self.add_count(delivery_order.get_code().clone(), count);
        delivery_order = delivery_order.with_count(count.to_string());
        if let Some(count) = self.get_count(delivery_order.get_code()) {
            if let Some(left_count) = left_count {
                if left_count != *count {
                    println!(
                        "Count not equal: {} <-> {} @date<{}>",
                        left_count,
                        count,
                        delivery_order.get_date(),
                    );
                }
            }
            delivery_order = delivery_order.with_owned(format!("{}", count));
        }

        delivery_order
    }

    pub async fn extract_from_file(
        &mut self,
        path: String,
        sender: Arc<Sender<Option<DeliveryOrder>>>,
    ) -> std::io::Result<()> {
        if path.ends_with("txt") {
            self.extract_from_file_impl(path, sender).await?;
        } else {
            panic!("Not support current file: {}", path);
        }
        Ok(())
    }

    async fn extract_from_file_impl(
        &mut self,
        path: String,
        sender: Arc<Sender<Option<DeliveryOrder>>>,
    ) -> std::io::Result<()> {
        let mut reader = BufReader::new(File::open(&path).await?);
        let mut buffer = Vec::with_capacity(4096);
        let gbk_encoder = GBK;
        let mut title: Vec<String> = vec![];

        if reader.read_until(0x0a as u8, &mut buffer).await? > 0 {
            let (line, _, _) = gbk_encoder.decode(&buffer);
            title = line.trim().split("\t").map(|v| String::from(v)).collect();
            buffer.clear();
        }
        if self.debug {
            println!("start extract data from file: {:?}", &path);
        }
        loop {
            let size = reader.read_until(0x0a as u8, &mut buffer).await?;

            if size > 0 {
                let (line, _, _) = gbk_encoder.decode(&buffer);

                // if self.debug {
                //     println!("read line => {}", line);
                // }
                let order = self.gen_order(&title, line.to_string());

                if order.is_valid() {
                    sender
                        .send(Some(order))
                        .await
                        .expect(&format!("Can't send data from read thread: {}!", &path));
                }
            } else {
                sender
                    .send(None)
                    .await
                    .expect(&format!("Can't send data to write thread"));
                if self.debug {
                    println!("extract file {} is over!", &path);
                }
                break;
            }

            buffer.clear();
        }

        Ok(())
    }
}

pub async fn extract_from_file(
    ctx: Arc<Mutex<Context>>,
    paths: Vec<String>,
    sender: Arc<Sender<Option<DeliveryOrder>>>,
    debug: bool,
) -> std::io::Result<()> {
    for path in paths {
        ctx.lock()
            .await
            .borrow_mut()
            .set_debug(debug)
            .extract_from_file(path, sender.clone())
            .await?;
    }
    Ok(())
}
