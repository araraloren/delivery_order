mod htsc;

use std::sync::atomic::AtomicI32;

use aopt::prelude::*;

use async_std::task::spawn;
use async_std::{
    channel::{bounded, Receiver},
    sync::Arc,
};

use xlsxwriter::{Workbook, XlsxError};

const HTSC_TYPE: &'static str = "HTSC";
const OUTPUT: &'static str = "output.xlsx";

#[async_std::main]
async fn main() -> color_eyre::Result<()> {
    tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    color_eyre::install()?;

    let (s, r) = bounded(128);
    let sender = Arc::new(s);
    let receiver = Arc::new(r);
    let mut set = SimpleSet::default()
        .with_default_creator()
        .with_default_prefix();
    let mut parser = SimpleParser::<UidGenerator>::default();

    set.add_opt("-t=s!")?
        .add_alias("--type")?
        .set_default_value(HTSC_TYPE.into())
        .commit()?;
    set.add_opt("-o=s")?
        .add_alias("--output")?
        .set_default_value(OUTPUT.into())
        .commit()?;

    let uid = set.add_opt("input=p@*")?.commit()?;
    let counter = Arc::new(AtomicI32::new(0));
    let counter_reader = counter.clone();

    parser.add_callback(
        uid,
        simple_pos_cb!(move |_, set, path, _, value| {
            let default_value = String::default();
            let file_type = if let Some(value) = set.get_value("--type")? {
                value.as_str().unwrap_or(&default_value).as_str()
            } else {
                HTSC_TYPE
            };

            match file_type {
                HTSC_TYPE => {
                    spawn(htsc::read_from_export_file(String::from(path), sender.clone()));
                }
                _ => {
                    panic!("Unknow file type: {}", file_type);
                }
            }
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            Ok(Some(value))
        }),
    );

    getopt!(&mut std::env::args().skip(1), set, parser)?;

    let output_name = set.get_value("--output")?.unwrap().as_str().unwrap();

    if counter_reader.load(std::sync::atomic::Ordering::SeqCst) > 0 {
        write_htsc_to_tzzb_excel(output_name.to_owned(), receiver.clone(), counter_reader).await?;
    }
    Ok(())
}

async fn write_htsc_to_tzzb_excel(
    path: String,
    rec: Arc<Receiver<Option<DeliveryOrder>>>,
    counter_reader: Arc<AtomicI32>,
) -> Result<(), XlsxError> {
    static TZZB_TITLE: [&'static str; 8] = [
        "成交日期",
        "证券代码",
        "证券名称",
        "交易类别",
        "成交数量",
        "成交价格",
        "发生金额",
        "证券余额",
    ];

    let workbook = Workbook::new(&path);
    let mut sheet = workbook.add_worksheet(None)?;
    let mut counter = 0;
    let mut read_stop_counter = 0;

    for idx in 0..TZZB_TITLE.len() {
        sheet.write_string(counter, idx as u16, TZZB_TITLE[idx], None)?;
    }

    loop {
        if let Some(order) = rec
            .recv()
            .await
            .expect("Unable to receive from read thread")
        {
            counter += 1;
            sheet.write_string(counter, 0, order.get_date(), None)?;
            sheet.write_string(counter, 1, order.get_code(), None)?;
            sheet.write_string(counter, 2, order.get_name(), None)?;
            sheet.write_string(counter, 3, order.get_kind(), None)?;
            sheet.write_string(counter, 4, order.get_count(), None)?;
            sheet.write_string(counter, 5, order.get_prize(), None)?;
            sheet.write_string(counter, 6, order.get_actual(), None)?;
            sheet.write_string(counter, 7, order.get_owned(), None)?;
        } else {
            read_stop_counter += 1;
            if read_stop_counter == counter_reader.load(std::sync::atomic::Ordering::SeqCst) {
                break;
            }
        }
    }

    println!("--> read count = {}, {:?}", counter, counter_reader);

    workbook.close()?;

    Ok(())
}

#[derive(Debug, Default, Clone)]
pub struct DeliveryOrder {
    code: String,
    name: String,
    date: String,
    kind: String,
    count: String,
    prize: String,
    actual: String,
    owned: String,
}

impl DeliveryOrder {
    pub fn set_code(&mut self, code: String) {
        self.code = code;
    }

    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }

    pub fn set_date(&mut self, date: String) {
        self.date = date;
    }

    pub fn set_kind(&mut self, kind: String) {
        self.kind = kind;
    }

    pub fn set_count(&mut self, count: String) {
        self.count = count;
    }

    pub fn set_prize(&mut self, prize: String) {
        self.prize = prize;
    }

    pub fn set_actual(&mut self, actual: String) {
        self.actual = actual;
    }

    pub fn set_owned(&mut self, owned: String) {
        self.owned = owned;
    }

    pub fn with_code(mut self, code: String) -> Self {
        self.code = code;
        self
    }

    pub fn with_name(mut self, name: String) -> Self {
        self.name = name;
        self
    }

    pub fn with_date(mut self, date: String) -> Self {
        self.date = date;
        self
    }

    pub fn with_kind(mut self, kind: String) -> Self {
        self.kind = kind;
        self
    }

    pub fn with_count(mut self, count: String) -> Self {
        self.count = count;
        self
    }

    pub fn with_prize(mut self, prize: String) -> Self {
        self.prize = prize;
        self
    }

    pub fn with_actual(mut self, actual: String) -> Self {
        self.actual = actual;
        self
    }

    pub fn with_owned(mut self, owned: String) -> Self {
        self.owned = owned;
        self
    }

    pub fn get_code(&self) -> &String {
        &self.code
    }

    pub fn get_name(&self) -> &String {
        &self.name
    }

    pub fn get_date(&self) -> &String {
        &self.date
    }

    pub fn get_kind(&self) -> &String {
        &self.kind
    }

    pub fn get_count(&self) -> &String {
        &self.count
    }

    pub fn get_prize(&self) -> &String {
        &self.prize
    }

    pub fn get_actual(&self) -> &String {
        &self.actual
    }

    pub fn get_owned(&self) -> &String {
        &self.owned
    }
}
